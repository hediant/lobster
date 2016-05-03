var Writer = require('../lib/writer');
var Topic = require('../lib/topic');
var moment = require('moment');

var metrics = [
	{
		"id" : 0,
		"name" : "metric1",
		"desc" : "",
		"ver" : 0,
		"keys" : [
			{
				"name" : "tag1"
			},
			{
				"name" : "tag2"
			},
			{
				"name" : "tag3"
			}
		]
	},
	{
		"id" : 1,
		"name" : "metric2",
		"desc" : "",
		"ver" : 0,
		"keys" : [
			{
				"name" : "tag1",
				"type" : "Number"
			},
			{
				"name" : "tag2",
				"type" : "Number"
			},
			{
				"name" : "tag3",
				"type" : "Number"
			},
			{
				"name" : "tag4",
				"type" : "Number"
			},
			{
				"name" : "tag5",
				"type" : "Number"
			}
		]
	}
];

var writer = new Writer();
var topics = [];
var topic_count = 10;

function run (){
	for (var i=1; i<=topic_count; i++ ){
		topics.push(new Topic("topic_" + i));
	}

	setInterval(function (){
		var metric = Math.floor(Date.now() / 60000) % 2 ? metrics[0] : metrics[1];
		//var metric = metrics[1];
		doWrite(metric);
	}, 1000);
}

function doWrite (metric){
	var data = {};
	metric.keys.forEach(function (key){
		data[key.name] = Math.floor(Math.random()*1000000);
	});

	topics.forEach(function (topic){
		writer.append(topic, data, metric);
	});

	console.log("[%s] Write %s topics(%s), %s records.", 
		moment().format("YYYY-MM-DD HH:mm:ss"), 
		topics.length, 
		metric.name,
		topics.length * Object.keys(data).length);

}

run();
