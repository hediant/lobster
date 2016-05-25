var Client = require('../../client');
var moment = require('moment');
var Q = require('q');

var client = new Client({
	"duplicates" : ["http://localhost:8002"]
});

var day_range = 7;  // 7 days
var parallel_count = 10;
var interval = 30 * 1000;
var system_count = 1000;
var field_names = ["a_1", "d_1", "a_11", "d_11", "a_100", "d_100"]

var calcSum = function (obj){
	var count = 0;
	for (var key in obj){
		count += obj[key].length;
	}

	return count;
}

var ReadOne = function (topic_name){
	return Q.Promise(function (resolve, reject){

		var start = Date.now();
		var query_end = moment().valueOf();
		var query_start = moment()-moment.duration(day_range, "days");		

		client.readRaw(topic_name, {
			"start" : query_start,
			"end" : query_end,
			"fields" : field_names
		}, function (err, data){
			var end = Date.now();
			if (err){
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
				reject(err);
			}
			else {
				console.log("Read %s, %s records, cost:%s ms.", topic_name, calcSum(data), (end - start));
				resolve();		
			}			
		})

	});
}

var doRead = function (){
	var callbacks = [];
	for (var i=0; i<parallel_count; i++){
		var topic = "system_" + Math.floor(Math.random()*system_count);
		callbacks.push(ReadOne(topic));
	}

	Q.allSettled(callbacks).then(function (results){
		console.log(results[0].state)
		console.log("=====================================");
	});
}

var run = function (){
	doRead();
	setInterval(doRead, interval);
}

run();