'use strict';

module.exports = exports = function (RED) {
	const vm = require('vm');
	const r = require('rethinkdb');
	const createSandbox = require('./sandbox');

	function RethinkdbOutNode(config) {
		RED.nodes.createNode(this, config);
		this.conf = RED.nodes.getNode(config.rethinkdbConfig);
		if (!this.conf || !this.conf.credentials) {
			this.status({fill: 'red', shape: 'dot', text: 'Missing RethinkDB config'});
			return;
		}
		this.status({fill: 'grey', shape: 'dot', text: 'Connecting'});
		this.connection = r.connect(this.conf.credentials);
		this.connection
			.then(conn => {
				this.conn = conn;
				this.status({fill: 'green', shape: 'dot', text: 'Connected'});
			})
			.catch(err => {
				this.conn = null;
				this.status({fill: 'red', shape: 'dot', text: err.message});
				this.error(err);
			});

		this.on('close', done => {
			this.status({fill: 'grey', shape: 'ring', text: 'Closed'});
			if (this.cursorToClose) {
				this.cursorToClose.close();
			}
			if (this.conn) {
				this.conn.close(done);
			} else {
				done();
			}
		});

		const sandbox = createSandbox(this);
		const context = vm.createContext(sandbox);

		try {
			const script = vm.createScript(`
				q = (function () {
					return ${config.query || null};
				})();
			`);
			script.runInContext(context);
		} catch (err) {
			this.status({fill: 'red', shape: 'dot', text: err.message});
			this.error(err);
		}

		const cursorMethod = config.asArray ? 'toArray' : 'eachAsync';

		const handleResult = result => {
			this.status({fill: 'green', shape: 'ring', text: 'Sending data'});
			this.send({payload: result});
			this.status({fill: 'green', shape: 'dot', text: 'Waiting'});
			return;
		};

		if (context.q) {
			this.connection
				.then(conn => {
					this.status({fill: 'yellow', shape: 'dot', text: 'Running query'});
					return context.q.run(conn);
				})
				.then(cursor => {
					this.status({fill: 'green', shape: 'dot', text: 'Waiting'});
					if (typeof cursor[cursorMethod] !== 'function') {
						return handleResult(cursor);
					}
					this.cursorToClose = cursor;
					let resultPromise;
					if (config.asArray) {
						resultPromise = cursor.toArray().then(handleResult);
					} else {
						resultPromise = cursor.eachAsync(handleResult);
					}
					return resultPromise
						.then(() => {
							this.status({fill: 'grey', shape: 'dot', text: 'Done'});
							this.cursorToClose = null;
						}, err => {
							this.cursorToClose = null;
							throw err;
						});
				})
				.catch(err => {
					this.status({fill: 'red', shape: 'dot', text: err.message});
					this.error(err);
				});
		}
	}
	RED.nodes.registerType('rethinkdb-out', RethinkdbOutNode);
};
