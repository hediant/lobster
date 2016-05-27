var Aggregation = require("../lib/aggregation");

Aggregation("topic_1", 0, Date.now(), function (err, results){
	console.log(err);
	console.log(results);
})