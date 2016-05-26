'use strict';

module.exports = exports = function createSandbox(node) {
	const r = require('rethinkdb');
	return {
		r,
		context: {
			set: function () {
				node.context().set.apply(node, arguments);
			},
			get: function () {
				return node.context().get.apply(node, arguments);
			},
			get global() {
				return node.context().global;
			},
			get flow() {
				return node.context().flow;
			}
		},
		flow: {
			set: function () {
				node.context().flow.set.apply(node, arguments);
			},
			get: function () {
				return node.context().flow.get.apply(node, arguments);
			}
		},
		global: {
			set: function () {
				node.context().global.set.apply(node, arguments);
			},
			get: function () {
				return node.context().global.get.apply(node, arguments);
			}
		}
	};
};
