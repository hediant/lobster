var assert = require('assert');
var request = require('request');
var parseBody = require('./parse_body');
var moment = require('moment');

var base = "http://localhost:8001";
var batch_count = 10;
var metrics = ["metric_test_1", "metric_test_2"];
var tag_count = 5;

var generateBatchData = function (){
	var data = [];
	for (var i=0; i<batch_count; i++){
		data.push(generateData(i));
	}

	return data;
}

var generateData = function (idx){
	var ret = {
		"topic" : "topic_" + (idx+1).toString(),
		"metric" : idx % 2 ? metrics[1] : metrics[0]
	}

	ret["data"] = {};
	for (var i=1; i<=tag_count; i++){
		ret["data"]["tag"+i] = Math.floor(Math.random()*1000000);
	}

	return ret;
}

var doAppendBatch = function (){
	var data = generateBatchData();
	var start = Date.now();

	request.post({
		url : base +"/append",
		json : data
	},function (err, res, body){
		assert(null == err);
		var end = Date.now();

		console.log("[%s] Write %s topics, %s records, err:%s, cost:%s ms.", 
			moment().format("YYYY-MM-DD HH:mm:ss"),
			data.length,
			data.length * tag_count,
			body.err,
			(end - start));
	});	
}

var run = function (){
	doAppendBatch();
	setInterval(doAppendBatch, 5000);
}

run();