var Writer = require('../lib/writer');
var Topic = require('../lib/topic');
var moment = require('moment');

var tag_count = 200;
var topic_count = 1000;
var rcd_per_hour = 60;

var metric = {
	"id" : 0,
	"name" : "metric1",
	"desc" : "",
	"ver" : 0,
	"keys" : []
}

var topics = [];
var writer = new Writer();

function init (){
	for (var i=0; i<tag_count; i++){
		metric.keys.push({
			"name" : "tag_" + (i+1).toString()
		});
	}

	for (var i=0; i<topic_count; i++){
		topics.push(new Topic("system_" + (i+1).toString()));
	}
}

init();

function doWrite (){
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

function run (){
	doWrite();
	setInterval(doWrite, 60*60*1000/rcd_per_hour);
}

run();