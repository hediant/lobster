var assert = require('assert');
var request = require('request');
var parseBody = require('./parse_body');
var Topic = require('../lib/topic');

var metrics = [
	{
		"id" : 0,
		"name" : "metric1",
		"desc" : "",
		"ver" : 0,
		"keys" : [
			{
				"name" : "tag1"
			},
			{
				"name" : "tag2"
			},
			{
				"name" : "tag3"
			}
		]
	},
	{
		"id" : 1,
		"name" : "metric2",
		"desc" : "",
		"ver" : 0,
		"keys" : [
			{
				"name" : "tag1",
				"type" : "String"
			},
			{
				"name" : "tag2",
				"type" : "Number"
			},
			{
				"name" : "tag3",
				"type" : "Location"
			}
		]
	},	
	{
		"id" : 2,
		"name" : "metric3",
		"desc" : "metric3",
		"ver" : 0,
		"keys" : []
	}	
];

var topics = [];

// create topic 0
describe("create topic 0", function(){
	it("should without error", function(done){
		var topic = new Topic("topic_0");
		topics[0] = topic;

		topic.on('ready', function (){
			done();
		})
	});
});

describe("get write stream from topic 0 of metric 0", function(){
	it("should without error", function(done){
		var topic = topics[0];
		topic.getWriteStream(metrics[0], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

describe("get write stream from topic 0 of metric 0", function(){
	it("should without error", function(done){
		var topic = topics[0];
		topic.getWriteStream(metrics[0], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

// create topic 1
describe("create topic 1", function(){
	it("should without error", function(done){
		var topic = new Topic("topic_1");
		topics[1] = topic;
		done();
	});
});

describe("get write stream from topic 1 of metric 1", function(){
	it("should without error", function(done){
		var topic = topics[1];
		topic.getWriteStream(metrics[1], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

describe("get write stream from topic 1 of metric 1", function(){
	it("should without error", function(done){
		var topic = topics[1];
		topic.getWriteStream(metrics[1], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

describe("get write stream from topic 1 of metric 2", function(){
	it("should without error", function(done){
		debugger
		var topic = topics[1];
		topic.getWriteStream(metrics[2], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

describe("get write stream from topic 1 of metric 0", function(){
	it("should without error", function(done){
		debugger
		var topic = topics[1];
		topic.getWriteStream(metrics[0], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

// create topic 2
describe("create topic 2", function(){
	it("should without error", function(done){
		var topic = new Topic("topic_2");
		topics[2] = topic;
		done();
	});
});

describe("get write stream from topic 2 of metric 2", function(){
	it("should without error", function(done){
		var topic = topics[2];
		topic.getWriteStream(metrics[2], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});

describe("get write stream from topic 2 of metric 2", function(){
	it("should without error", function(done){
		var topic = topics[2];
		topic.getWriteStream(metrics[2], function (err, stream){
			assert(err==null);
			if (!err)
				stream.end();
			done();
		})
	});
});
