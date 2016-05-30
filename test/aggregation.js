var Aggregation = require("../lib/aggregation");

var topic_name = "topic_1";

var st = Date.now();
Aggregation(topic_name, 0, Date.now(), function (err, results){
	var ed = Date.now();
	console.log("Aggregation %s, cost:%s ms, err:%s.",
		topic_name,
		(ed - st),
		err);
	if (results){
		console.log(Object.keys(results));
	}
});