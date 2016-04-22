var assert = require('assert');
var request = require('request');
var parseBody = require('./parse_body');

var base = "http://localhost:8001";

var metrics = [
	{
		"name" : "metric1",
		"desc" : "",
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
		"name" : "metric2",
		"desc" : "",
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
		"name" : "metric3",
		"desc" : "metric3",
		"keys" : []
	},
]

// create metric 0
describe("create metric 0", function(){
	it("should without error", function(done){
		console.log(metrics[0]);
		request.post({
			url : base +"/metrics",
			json : metrics[0]
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			metrics[0].id = parseBody(body).ret;
			done();
		});
	});
});

// create metric 1
describe("create metric 1", function(){
	it("should without error", function(done){
		console.log(metrics[1]);
		request.post({
			url : base +"/metrics",
			json : metrics[1]
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			metrics[1].id = parseBody(body).ret;
			done();
		});
	});
});

// create metric 2
describe("create metric 2", function(){
	it("should without error", function(done){
		console.log(metrics[2]);
		request.post({
			url : base +"/metrics",
			json : metrics[2]
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			metrics[2].id = parseBody(body).ret;
			done();
		});
	});
});

// get metric 0
describe("get metric 0", function(){
	it("should without error", function(done){
		request.get({
			url : base +"/metrics/" + metrics[0].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// get metric 1
describe("get metric 1", function(){
	it("should without error", function(done){
		request.get({
			url : base +"/metrics/" + metrics[1].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// get metric 2
describe("get metric 2", function(){
	it("should without error", function(done){
		request.get({
			url : base +"/metrics/" + metrics[2].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// set metric 2
describe("set metric 2", function(){
	it("should without error", function(done){
		request.put({
			url : base +"/metrics/" + metrics[2].id,
			json : {
				"keys" : [
					{
						"name" : "tag1",
						"type" : "String"
					}
				]
			}
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// get metric 2
describe("get metric 2", function(){
	it("should without error", function(done){
		request.get({
			url : base +"/metrics/" + metrics[2].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// find metric
describe("find metric", function(){
	it("should without error", function(done){
		request.get({
			url : base +"/metrics"
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// delete metric 0
describe("delete metric 0", function(){
	it("should without error", function(done){
		request.del({
			url : base +"/metrics/" + metrics[0].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// delete metric 1
describe("delete metric 1", function(){
	it("should without error", function(done){
		request.del({
			url : base +"/metrics/" + metrics[1].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});

// delete metric 2
describe("delete metric 2", function(){
	it("should without error", function(done){
		request.del({
			url : base +"/metrics/" + metrics[2].id
		},function (err, res, body){
			assert(null == err);
			console.log(body);
			done();
		});
	});
});
