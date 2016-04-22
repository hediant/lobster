var Orm = require("../../simpleorm").Orm;

exports.metricsOrm = new Orm("t_metrics", {
	"id" : { "alias":"id", "readonly":true, "auto":true },
	"name" : { "alias":"name" },
	"desc" : { "alias":"desc" },
	"ver" : { "alias":"ver", "readonly":true },
	"keys" : { "alias":"keys"},
	"create_time" : { "alias":"create_time", "readonly":true, "auto":true },
	"modify_time" : { "alias":"modify_time", "readonly":true, "auto":true }
});

exports.updatesOrm = new Orm("t_updates", {
	"ver" : { "alias":"ver", "readonly":true, "auto":true },
	"topic" : { "alias":"topic", "readonly":true, "auto":true },
	"id" : { "alias":"id", "readonly":true, "auto":true },
	"op" : { "alias":"op", "readonly":true, "auto":true },
	"ts" : { "alias":"ts", "readonly":true, "auto":true }
});