var Reader = require('../lib/reader');
var moment = require('moment');
var co = require('co');

var topic_name = "topic_1";

var doRead = function (cb){
	var reader = new Reader(topic_name);
	var start = moment() - moment.duration(2, "days");

	var cursor = reader.cursor(start);
	cursor.on('ready', function (){
		var results = [];

		co(function *(){
			var data = yield cursor.next();
			while(data){
				results.push(data);
				console.log(data);

				data = yield cursor.next();
			}

			return cb(null, results);

		}).catch(function (err){
			return cb(err);
		});
	});
}

var run = function (){
	var s = Date.now();
	doRead(function (err, results){
		var e = Date.now();
		if (err){
			console.log("Traversal cursor error:", err);
		}
		else{
			console.log("Traversal cursor completed. results len:%s, cost:%s ms.",
				results.length,
				(e - s));
		}
	})
}

/*
	NOTE:
	* Need ES6 support
	* To run this case, use "node -harmony cursor.js"
*/
run();