var Reader = require('../lib/reader');
var moment = require('moment');
var assert = require('assert');

var topic_name = "topic_1";
var tag1 = "tag1";
var tag2 = "tag2";

var field_names = [tag1, tag2, "NOT_EXISTS"];

var readRaw = function (start, end, options, cb){
	var reader = new Reader(topic_name, field_names);
	var t1 = Date.now();

	reader.readRaw(start, end, options, function (err, data){
		var t2 = Date.now();
		if (err)
			console.log("Read %s error: %s.", topic_name, err);
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

describe("readRaw 14, with where func, 24 hours", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day) - moment.duration(12, "hours");
		var end = moment(day) + moment.duration(12, "hours");

		var options = {
			"where" : function (row){return row[tag1] < 10000;}
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 15, with where func, 24 hours, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day) - moment.duration(12, "hours");
		var start = moment(day) + moment.duration(12, "hours");

		var options = {
			"where" : function (row){return row[tag1] < 10000;}
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 16, limit, 24 hours", function(){
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
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 17, limit, 24 hours, backward", function(){
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
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"))
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 18, read a week", function(){
	it("should without error", function(done){
		var start = moment() - moment.duration(1, "weeks");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 19, read a week, backward", function(){
	it("should without error", function(done){
		var end = moment() - moment.duration(1, "weeks");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(!err == true);
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 20, topic not exists.", function(){
	it("should return ER_TOPIC_NOT_EXIST", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var topic_name = "TOPIC_NOT_EXIST";
		var reader = new Reader(topic_name, field_names);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err);
			else {
				console.log("Read %s, %s records, cost:%s ms.", topic_name, data.length, (t2 - t1));		
			}

			assert(err == "ER_TOPIC_NOT_EXIST");
			done();
		});	
	});
});

describe("readRaw 21, topic not exists. backward", function(){
	it("should return ER_TOPIC_NOT_EXIST", function(done){
		var end = moment() - moment.duration(2, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var topic_name = "TOPIC_NOT_EXIST";
		var reader = new Reader(topic_name, field_names);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err);
			else {
				console.log("Read %s, %s records, cost:%s ms.", topic_name, data.length, (t2 - t1));		
			}

			assert(err == "ER_TOPIC_NOT_EXIST");
			done();
		});	
	});
});

describe("readRaw 22, read 8 days", function(){
	it("should return ER_BAD_DATE_RANGE", function(done){
		var start = moment() - moment.duration(8, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(err == "ER_BAD_DATE_RANGE");
			done();
		})
	});
});

describe("readRaw 23, read 8 days, backward", function(){
	it("should return ER_BAD_DATE_RANGE", function(done){
		var end = moment() - moment.duration(8, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, undefined, function (err, data){
			assert(err == "ER_BAD_DATE_RANGE");
			done();
		})
	});
});

describe("readRaw 24, read [] fields.", function(){
	it("should return empty data", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var reader = new Reader(topic_name, []);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();
			assert(err == null);
			assert(0 == Object.keys(data).length);
			done();
		});	
	});
});

describe("readRaw 25, read [] fields. backward", function(){
	it("should return should return empty data", function(done){
		var end = moment() - moment.duration(2, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var reader = new Reader(topic_name, []);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();

			assert(err == null);
			assert(0 == Object.keys(data).length);
			done();
		});	
	});
});

describe("readRaw 26, read all fields.", function(){
	it("should return all fields data", function(done){
		var start = moment() - moment.duration(2, "days");
		var end = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var reader = new Reader(topic_name);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err == null);
			done();
		});	
	});
});

describe("readRaw 27, read all fields. backward", function(){
	it("should return all fields data", function(done){
		var end = moment() - moment.duration(2, "days");
		var start = moment().valueOf();
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		var reader = new Reader(topic_name);
		var t1 = Date.now();

		reader.readRaw(start, end, function (err, data){
			var t2 = Date.now();
			if (err)
				console.log("Read %s error: %s.", topic_name, err);
			else {
				console.log("Read %s, %s fields, cost:%s ms.", topic_name, Object.keys(data).length, (t2 - t1));		
			}

			assert(err == null);
			done();
		});	
	});
});

describe("readRaw 28, with where func, 24 hours and limit", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var start = moment(day) - moment.duration(12, "hours");
		var end = moment(day) + moment.duration(12, "hours");

		var options = {
			"where" : function (row){return row[tag1] < 10000;},
			"limit" : 5
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"), row[1])
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});

describe("readRaw 29, with where func, 24 hours and limit, backward", function(){
	it("should without error", function(done){
		var day = moment().format("YYYY-MM-DD");

		var end = moment(day) - moment.duration(12, "hours");
		var start = moment(day) + moment.duration(12, "hours");

		var options = {
			"where" : function (row){return row[tag1] < 10000;},
			"limit" : 5
		}
		
		console.log("Start:\t%s", moment(start).format("YYYY-MM-DD HH:mm:ss"));
		console.log("End:\t%s", moment(end).format("YYYY-MM-DD HH:mm:ss"));

		readRaw(start, end, options, function (err, data){
			assert(!err == true);
			data[tag1].forEach(function (row){
				console.log(moment(row[0]).format("YYYY-MM-DD HH:mm:ss"), row[1])
			})
			console.log("data count:", data[tag1].length);
			done();
		})
	});
});