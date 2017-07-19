'use strict';
module.exports = exports = function (RED) {
	function RethinkdbConfig(config) {
		RED.nodes.createNode(this, config);
	}
	RED.nodes.registerType('rethinkdb config', RethinkdbConfig, {
		credentials: {
			host: {type: 'text'},
			port: {type: 'text'},
			db: {type: 'text'},
			authKey: {type: 'text'},
			user: {type: 'text'},
			password: {type: 'text'},
			timeout: {type: 'number'}
		}
	});
};
