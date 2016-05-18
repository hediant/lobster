var assert = require('assert');
var request = require('request');
var parseBody = require('./parse_body');
var moment = require('moment');

var base = "http://localhost:8001";
var metrics = ["metric_test_1", "metric_test_2"];
var tag_count = 5;
var topic_count = 10;
var topics = [];

for (var i=0; i<topic_count; i++){
	topics.push("topic_" + i);
}

var generateData = function (){
	var ret = {};
	for (var i=1; i<=tag_count; i++){
		ret["tag"+i] = Math.floor(Math.random()*1000000);
	}

	return ret;
}

var append_ = function (topic_name, metric_name){
	var data = generateData();
	var start = Date.now();

	request.post({
		url : base +"/topics/" + topic_name + "/append?metric=" + metric_name,
		json : data
	},function (err, res, body){
		assert(null == err);
		var end = Date.now();

		console.log("[%s] Write topic: %s, metric: %s, err: %s, cost: %s ms.", 
			moment().format("YYYY-MM-DD HH:mm:ss"),
			topic_name,
			metric_name,
			body.err,
			(end - start));
	});	
}

var doAppend = function (){
	topics.forEach(function (topic_name){
		var metric = Math.floor(Date.now() / 60000) % 2 ? metrics[0] : metrics[1];
		append_(topic_name, metric);
	});
}

var run = function (){
	doAppend();
	setInterval(doAppend, 5000);
}

run();