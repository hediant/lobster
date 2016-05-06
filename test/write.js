var Writer = require('../lib/writer');
var Topic = require('../lib/topic');
var moment = require('moment');
var Q = require('q');
var Metric = require('../lib/metric');

var metrics = [];

var writer = new Writer();
var topics = [];
var topic_count = 10;

function run (){
	for (var i=1; i<=topic_count; i++ ){
		topics.push(new Topic("topic_" + i));
	}

	Q.fcall(function (){
		return Q.Promise(function (resolve, reject){
			Metric.find({"name":"metric_test_1"}, function (err, results){
				if (err){
					reject(err);
				}
				else if(!results.length){
					reject("metric_test_1 not exist.");
				}
				else{
					metrics.push(results[0]);
					resolve();
				}
			})
		})
	}).then(function (){
		return Q.Promise(function (resolve, reject){
			Metric.find({"name":"metric_test_2"}, function (err, results){
				if (err){
					reject(err);
				}
				else if(!results.length){
					reject("metric_test_2 not exist.");
				}				
				else{
					metrics.push(results[0]);
					resolve();
				}
			})
		})
	}).then(function (){
		setInterval(function (){
			var metric = Math.floor(Date.now() / 60000) % 2 ? metrics[0] : metrics[1];
			//var metric = metrics[1];
			doWrite(metric);
		}, 1000);		
	}).catch(function (err){
		console.log(err);
		process.exit(1);
	});
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
