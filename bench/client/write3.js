var moment = require('moment');
var Client = require('../../client');
var Q = require('q');
var EventEmitter = require('events').EventEmitter;
var client = new Client();
//
// SETTINGS
// 
var topic_count = 1000;
var topics = [];
var metric_name = "metric_bench_3";
var metric;

var digital_tags_count = 25;
var dtags = {
	"fast" : [],
	"fast_count" : 5,
	"slow" : [],
	"slow_count" : 10,
	"very_slow" : [],
	"very_slow_count" : 10
}

var analog_tags_count = 25;
var atags = {
	"fast" : [],
	"fast_count" : 5,
	"slow" : [],
	"slow_count" : 10,
	"very_slow" : [],
	"very_slow_count" : 10
}

var fast_interval = 30 * 1000;
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

function Writer(client){
	EventEmitter.call(this);
	var me = this;
	var client_ = client;
	var queue_ = [];
	var max_queue_len_ = 100;
	var timer_ = null;
	var commit_cycle_ = 5 * 1000;

	var calc_sum_ = function (queue){
		var count = 0;
		queue.forEach(function (it){
			count += Object.keys(it.data).length;
		});

		return count;
	}

	var commit_ = function (queue){
		if (!queue.length)
			return;

		(function (){
			var start = Date.now();
			client_.append(queue, function (err, ret){
				var end = Date.now();
				console.log("[%s] Write records:%s, cost:%s ms, status:%s.",
					moment().format("YYYY-MM-DD HH:mm:ss"),
					calc_sum_(queue),
					(end - start),
					err ? (err.message ? err.message : err.code) : "OK");
			});			
		})();

	}

	this.append = function (topic_name, metric_name, data, timestamp){
		queue_.push({
			"topic" : topic_name,
			"metric" : metric_name,
			"data" : data,
			"ts" : timestamp
		});

		if (queue_.length >= max_queue_len_){
			commit_(queue_);
			queue_ = [];
		}
	}

	this.run = function (){
		if (timer_)
			clearInterval(timer_);

		timer_ = setInterval(function(){
			console.log("=================================================================");
			console.log("[%s] Time to check queue.", moment().format("YYYY-MM-DD HH:mm:ss"));

			commit_(queue_);
		}, commit_cycle_);
	}

	this.close = function (){
		if (timer_)
			clearInterval(timer_);
		me.removeAllListeners();
	}

	this.run();
}
require('util').inherits(Writer, EventEmitter);

// 
// Write Instance
//
var writer = new Writer(client);

//
// Construct data
//
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

	topics.forEach(function (topic_name){
		setImmediate(function (){
			writer.append(topic_name, metric.name, data, ts);
		})
	});
}

function run (){
	Q.fcall(function (){
		return Q.Promise(function (resolve, reject){
			client.getMetric(metric_name, function (err, result){
				if (err)
					reject(err);			
				else{
					metric = result;
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