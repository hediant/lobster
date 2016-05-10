var Reader = require('../lib/reader');
var moment = require('moment');
var assert = require('assert');

var topic_name = "system_100";
var field_names = ["tag_1", "tag_2", "NOT_EXISTS"];

var readRaw = function (start, end, options, cb){
	var reader = new Reader(topic_name, field_names);

	var t1 = Date.now();
	/*
	var query_start = moment()-moment.duration(3, "days");
	var query_end = moment().valueOf();
	var options = {
		where : function (row){return row["tag_1"] < 10000;}
	}
	*/
	reader.readRaw(start, end, options, function (err, data){
		var t2 = Date.now();
		if (err)
			console.log("Read %s error: %s.", topic_name, err);
		else {
			console.log("Read %s, %s records, cost:%s ms.", topic_name, data.length, (t2 - t1));		
		}

		cb(err, data);
	});	
}

describe("readRaw 1", function(){
	it("should without error", function(done){
		var start = moment()-moment.duration(3, "days");
		var end = moment().valueOf();
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			//console.log(data[0])
			console.log(moment(data[0].__ts__).format("YYYY-MM-DD HH:mm:ss"));
			done();
		})
	});
});

describe("readRaw 2, backword", function(){
	it("should without error", function(done){
		var end = moment()-moment.duration(3, "days");
		var start = moment().valueOf();
		debugger
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			//console.log(data[0])
			console.log(moment(data[0].__ts__).format("YYYY-MM-DD HH:mm:ss"));
			console.log(moment(data[1].__ts__).format("YYYY-MM-DD HH:mm:ss"));
			console.log(moment(data[2].__ts__).format("YYYY-MM-DD HH:mm:ss"));
			done();
		})
	});
});