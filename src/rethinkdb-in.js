'use strict';

module.exports = exports = function (RED) {
	const vm = require('vm');
	const r = require('rethinkdb');
	const createSandbox = require('./sandbox');

	function RethinkdbInNode(config) {
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
			if (this.conn) {
				this.conn.close(done);
			} else {
				done();
			}
		});

		const sandbox = createSandbox(this);

		try {
			const script = vm.createScript(`
				q = (function (msg) {
					return ${config.query || null};
				})(msg);
			`);
			this.on('input', msg => {
				const context = Object.assign({msg}, sandbox);
				try {
					script.runInNewContext(context);
				} catch (err) {
					this.error(err, msg);
				}
				if (context.q) {
					this.connection
						.then(conn => {
							this.status({fill: 'yellow', shape: 'dot', text: 'Running query'});
							return context.q.run(conn);
						})
						.then(() => {
							this.status({fill: 'green', shape: 'dot', text: 'Idle'});
						})
						.catch(err => {
							this.status({fill: 'red', shape: 'dot', text: err.message});
							this.error(err, msg);
						});
				}
			});
		} catch (err) {
			this.error(err);
		}
	}
	RED.nodes.registerType('rethinkdb-in', RethinkdbInNode);
};
