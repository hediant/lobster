var Client = require('../client');
var moment = require('moment');
var assert = require('assert');

var topic_name = "topic_1";
var tag1 = "tag1";
var tag2 = "tag2";

var field_names = [tag1, tag2, "NOT_EXISTS"];

var client = new Client();

var calcRecords_ = function (data){
	var count = 0;
	for (var key in data){
		count += data[key].length;
	}

	return count;
}

var readRaw = function (start, end, options, cb){
	var t1 = Date.now();
	var query = {
		"start" : moment(start).valueOf(),
		"fields" : field_names
	}
	if (end)
		query["end"] = moment(end).valueOf();

	if (options && options.limit){
		query["limit"] = options.limit
	}

	client.readRaw(topic_name, query, function (err, data){
		var t2 = Date.now();
		if (err)
			console.log("Read %s error: %s.", topic_name, err ? (err.message ? err.message : err.code) : null);
		else {
			console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
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
			assert(!err);
			assert(2 == Object.keys(data).length);
			console.log(moment(data[tag1][0][0]).format("YYYY-MM-DD HH:mm:ss"));
			done();
		})
	});
});

describe("readRaw 2, backword", function(){
	it("should without error", function(done){
		var end = moment()-moment.duration(3, "days");
		var start = moment().valueOf();

		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err);
			console.log(moment(data[tag1][1][0]).format("YYYY-MM-DD HH:mm:ss"));
			console.log(moment(data[tag1][2][0]).format("YYYY-MM-DD HH:mm:ss"));
			console.log(moment(data[tag1][3][0]).format("YYYY-MM-DD HH:mm:ss"));
			done();
		})
	});
});

describe("readRaw 3, start == end", function(){
	it("should without error", function(done){
		var end = moment()-moment.duration(1, "hours");
		var start = end;

		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err);
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 4, 10 minutes", function(){
	it("should without error", function(done){
		var start = moment()-moment.duration(10, "minutes");
		var end = moment();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 5, 10 minutes before 10 minutes", function(){
	it("should without error", function(done){
		var start = moment()-moment.duration(20, "minutes");
		var end = moment()-moment.duration(10, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 6, 10 minutes, backward", function(){
	it("should without error", function(done){
		var end = moment()-moment.duration(10, "minutes");
		var start = moment();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 7, 10 minutes before 10 minutes, backward", function(){
	it("should without error", function(done){
		var end = moment()-moment.duration(20, "minutes");
		var start = moment()-moment.duration(10, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 8, 10 minutes across the two days", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day) - moment.duration(5, "minutes");
		var end = moment(day) + moment.duration(5, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 9, 10 minutes across the two day, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day) - moment.duration(5, "minutes");
		var start = moment(day) + moment.duration(5, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 10, last 10 minutes of yesterday", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day) - moment.duration(10, "minutes");
		var end = moment(day);
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 11, last 10 minutes of yesterday, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day) - moment.duration(10, "minutes");
		var start = moment(day);
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 12, 10 minutes earlier today", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day);
		var end = moment(day) + moment.duration(10, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 13, 10 minutes earlier today, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day);
		var start = moment(day) + moment.duration(10, "minutes");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 14, limit, 24 hours", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day) - moment.duration(12, "hours");
		var end = moment(day) + moment.duration(12, "hours");

		var options = {
			"limit" : 10
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err == true);
			assert(calcRecords_(data)<=10)
			done();
		})
	});
});

describe("readRaw 15, limit, 24 hours, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day) - moment.duration(12, "hours");
		var start = moment(day) + moment.duration(12, "hours");

		var options = {
			"limit" : 10
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err == true);
			assert(calcRecords_(data)<=10)
			done();
		})
	});
});

describe("readRaw 16, read a week", function(){
	it("should without error", function(done){
		var end = moment().valueOf();
		var start = moment() - moment.duration(1, "weeks");		
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 17, read a week, backward", function(){
	it("should without error", function(done){
		var start = moment().valueOf();
		var end = moment() - moment.duration(1, "weeks");
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 18, topic not exists.", function(){
	it("should return ER_TOPIC_NOT_EXIST", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var topic_name = "TOPIC_NOT_EXIST";
		var t1 = Date.now();

		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s records, cost:%s ms.", topic_name, data.length, (t2 - t1));		
			}

			assert(err.code == "ER_TOPIC_NOT_EXIST");
			done();
		});	
	});
});

describe("readRaw 19, topic not exists. backward", function(){
	it("should return ER_TOPIC_NOT_EXIST", function(done){
		var end = moment() - moment.duration(2, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var topic_name = "TOPIC_NOT_EXIST";
		var t1 = Date.now();

		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s records, cost:%s ms.", topic_name, data.length, (t2 - t1));		
			}

			assert(err.code == "ER_TOPIC_NOT_EXIST");
			done();
		});	
	});
});

describe("readRaw 20, read 8 days", function(){
	it("should return ER_BAD_DATE_RANGE", function(done){
		var start = moment() - moment.duration(8, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(err.code == "ER_BAD_DATE_RANGE");
			done();
		})
	});
});

describe("readRaw 21, read 8 days, backward", function(){
	it("should return ER_BAD_DATE_RANGE", function(done){
		var end = moment() - moment.duration(8, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(err.code == "ER_BAD_DATE_RANGE");
			done();
		})
	});
});

describe("readRaw 22, read all fields.", function(){
	it("should return all fields data", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err == null);
			done();
		});	
	});
});

describe("readRaw 23, read all fields. backward", function(){
	it("should return all fields data", function(done){
		var end = moment() - moment.duration(2, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err == null);
			done();
		});	
	});
});

describe("readRaw 24, read bad start time.", function(){
	it("should return err ER_INVALID_START_TIME", function(done){
		var start = undefined;
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err && err.code == "ER_INVALID_START_TIME")
			done();
		});	
	});
});

describe("readRaw 25, read bad end time.", function(){
	it("should return err ER_INVALID_END_TIME", function(done){
		var start = moment().valueOf();
		var end = undefined;
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end
		}

		client.readRaw(topic_name, query, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err ? err.code : null);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err && err.code == "ER_INVALID_END_TIME")
			done();
		});	
	});
});

describe("readRaw 26, read all fields. pipeline", function(){
	it("should return all fields data", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end,
			"limit" : 10
		}

		var stream = client.readRawStream(topic_name, query);
		stream.on('end', done);
		stream.pipe(process.stdout);
	});
});

describe("readRaw 27, read all fields. pipeline events", function(){
	it("should return all fields data", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var t1 = Date.now();
		var query = {
			"start" : start,
			"end" : end,
			"limit" : 10
		}

		var data = "";
		var stream = client.readRawStream(topic_name, query);
		stream.on('end', function (){
			assert(calcRecords_(JSON.parse(data).ret) <= 10);
			done();
		});
		stream.on('data', function (trunk){
			data += trunk.toString();
		});
		stream.on('error', function (err){
			console.log(err);
			done();
		})
	});
});