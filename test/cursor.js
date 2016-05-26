var Reader = require('../lib/reader');
var moment = require('moment');
var co = require('co');
var Q = require('q');

var topic_name = "topic_1";

var doRead = function (start, forward, cb){
	var reader = new Reader(topic_name);
	var cursor = reader.cursor(start, {"forward":forward});
	cursor.on('ready', function (){
		var results = [];

		co(function *(){
			var data = yield cursor.next();
			while(data){
				results.push(data);
				//console.log(data);

				data = yield cursor.next();
			}

			cursor.close();
			return cb(null, results);

		}).catch(function (err){
			cursor.close();
			return cb(err);
		});
	});	

	cursor.on('error', function (err){
		cb (err);
	})

}

var run = function (){
	var s = Date.now();
	Q.fcall(function (){
		var start = 0;
		doRead(start, true, function (err, results){
			console.log("============ Forward ==============");
			var e = Date.now();
			if (err){
				console.log("Traversal cursor error:", err);
				return Q.Promise.reject(err);
			}
			else{
				console.log("Traversal cursor completed. results len:%s, cost:%s ms.",
					results.length,
					(e - s));
				return Q.Promise.resolve();
			}
		});
	}).then(function (){
		var start = Date.now();
		doRead(start, false, function (err, results){
			console.log("============ Backward ==============");
			var e = Date.now();
			if (err){
				console.log("Traversal cursor error:", err);
				return Q.Promise.reject(err);
			}
			else{
				console.log("Traversal cursor completed. results len:%s, cost:%s ms.",
					results.length,
					(e - s));
				return Q.Promise.resolve();
			}
		});
	}).catch(function (err){
		console.log(err);
	});

}

/*
	NOTE:
	* Need ES6 support
	* To run this case, use "node -harmony cursor.js"
*/
run();