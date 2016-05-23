var moment = require('moment');
var Metric = require('../lib/metric');
var Q = require('q');
var G = require('../global');

var topic_count = 50000;

var topics = [];
var metric;

var digital_tags_count = 100;
var dtags = {
	"fast" : [],
	"fast_count" : 10,
	"slow" : [],
	"slow_count" : 40,
	"very_slow" : [],
	"very_slow_count" : 50
}

var analog_tags_count = 100;
var atags = {
	"fast" : [],
	"fast_count" : 10,
	"slow" : [],
	"slow_count" : 40,
	"very_slow" : [],
	"very_slow_count" : 50
}

var fast_interval = 10 * 1000;
var slow_interval = 60 * 1000;
var very_slow_interval = 5 * 60 * 1000;

function init (){
	for (var i=0; i<topic_count; i++){
		topics.push("system_" + (i+1).toString());
	}

	for (var i=0; i<digital_tags_count; i++){
		if (i<dtags.fast_count){
			dtags.fast.push("d_" + (i+1).toString());
		}

		if (i>=dtags.fast_count && i< (dtags.slow_count + dtags.fast_count)){
			dtags.slow.push("d_" + (i+1).toString());
		}

		if (i>dtags.slow_count && i< (dtags.slow_count + dtags.fast_count + dtags.very_slow_count)){
			dtags.very_slow.push("d_" + (i+1).toString());
		}		
	}
	console.log("digital_tags");
	console.log(dtags);

	for (var i=0; i<analog_tags_count; i++){
		if (i<atags.fast_count){
			atags.fast.push("a_" + (i+1).toString());
		}

		if (i>=atags.fast_count && i<(atags.slow_count + atags.fast_count)){
			atags.slow.push("a_" + (i+1).toString());
		}

		if (i>=atags.slow_count && i<(atags.slow_count + atags.fast_count + atags.very_slow_count)){
			atags.very_slow.push("a_" + (i+1).toString());
		}		
	}

	console.log("analog_tags");
	console.log(atags);	
}

init();

var last_fast = 0;
var last_slow = 0;
var last_very_slow = 0;

function doWrite (){
	var ts = Date.now();
	var data = {};

	var fast = [];
	var slow = [];
	var very_slow = [];

	var data_tag_count = 0;

	metric.keys.forEach(function (key){
		var tag_name = key.name;

		if (ts - last_fast >= fast_interval){
			if (dtags.fast.indexOf(tag_name) != -1){
				fast.push([tag_name, Math.floor(Math.random()*10)])
			}

			if (atags.fast.indexOf(tag_name) != -1){
				fast.push([tag_name, (Math.random()*1000000).toFixed(2)])
			}
		}
		
		if (ts - last_slow >= slow_interval){
			if (dtags.slow.indexOf(tag_name) != -1){
				slow.push([tag_name, Math.floor(Math.random()*10)])
			}

			if (atags.slow.indexOf(tag_name) != -1){
				slow.push([tag_name, (Math.random()*1000000).toFixed(2)])
			}
		}

		if (ts - last_very_slow >= very_slow_interval){
			if (dtags.very_slow.indexOf(tag_name) != -1){
				very_slow.push([tag_name, Math.floor(Math.random()*10)])
			}

			if (atags.very_slow.indexOf(tag_name) != -1){
				very_slow.push([tag_name, (Math.random()*1000000).toFixed(2)])
			}
		}		
	});

	if (fast.length){
		last_fast = ts;
		fast.forEach(function (it){
			data[it[0]] = it[1];
			data_tag_count ++;
		})
	}

	if (slow.length){
		last_slow = ts;
		slow.forEach(function (it){
			data[it[0]] = it[1];
			data_tag_count ++;
		})
	}

	if (very_slow.length){
		last_very_slow = ts;
		very_slow.forEach(function (it){
			data[it[0]] = it[1];
			data_tag_count ++;
		})
	}

	if (!data_tag_count)
		return;

	topics.forEach(function (topic){
		G.getApp().append(topic, data, metric.name, ts);
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
			Metric.find({"name":"metric_bench_2"}, function (err, results){
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
		setInterval(doWrite, 1000);		
	}).catch(function (err){
		console.error(err);
		process.exit(1);
	});
}

run();