var moment = require('moment');
var Q = require('q');
var Metric = require('../lib/metric');
var G = require('../global');

var topic_count = 1000;
var rcd_per_hour = 60;

var metric;
var topics = [];

function init (){
	for (var i=0; i<topic_count; i++){
		topics.push("system_" + (i+1).toString());
	}
}

init();

function doWrite (){
	var data = {};
	metric.keys.forEach(function (key){
		data[key.name] = Math.floor(Math.random()*1000000);
	});

	topics.forEach(function (topic){
		G.getApp().append(topic, data, metric.name, Date.now());
	});

	console.log("[%s] Write %s topics(%s), %s records.", 
		moment().format("YYYY-MM-DD HH:mm:ss"), 
		topics.length, 
		metric.name,
		topics.length * Object.keys(data).length);	
}

function run (){
	Q.fcall(function (){
		return Q.Promise(function (resolve, reject){
			Metric.find({"name":"metric_bench_1"}, function (err, results){
				if (err)
					reject(err);
				else if(!results.length){
					reject("metric_bench_1 not exist.");
				}				
				else{
					metric = results[0];
					resolve();
				}
			})
		})
	}).then(function (){
		doWrite();
		setInterval(doWrite, 60*60*1000/rcd_per_hour);		
	}).catch(function (err){
		console.error(err);
		process.exit(1);
	});
}

run();