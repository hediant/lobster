var Metric = require('../lib/metric');
var assert = require('assert');

var tag_count = 200;
var digital_count = 100;
var analog_count = 100;

var metrics = [
	{
		"name" : "metric_test_1",
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
		"name" : "metric_test_2",
		"desc" : "",
		"ver" : 0,
		"keys" : [
			{
				"name" : "tag1",
				"type" : "Number"
			},
			{
				"name" : "tag2",
				"type" : "Number"
			},
			{
				"name" : "tag3",
				"type" : "Number"
			},
			{
				"name" : "tag4",
				"type" : "Number"
			},
			{
				"name" : "tag5",
				"type" : "Number"
			}
		]
	},
	{
		"name" : "metric_bench_1",
		"desc" : "",
		"ver" : 0,
		"keys" : []
	},
	{
		"name" : "metric_bench_2",
		"desc" : "",
		"ver" : 0,
		"keys" : []
	}	
];

function init(){
	for (var i=0; i<tag_count; i++){
		metrics[2].keys.push({
			"name" : "tag_" + (i+1).toString()
		});
	}

	for (var i=0; i<digital_count; i++){
		metrics[3].keys.push({
			"name" : "d_" + (i+1).toString(),
			"type" : "Digital"
		});
	}

	for (var i=0; i<analog_count; i++){
		metrics[3].keys.push({
			"name" : "a_" + (i+1).toString(),
			"type" : "Analog"
		})
	}
}
init();

describe("create " + metrics[0].name, function(){
	it("should without error", function(done){
		Metric.create(metrics[0], function (err){
			assert(!err || err == "ER_METRIC_EXIST");
			done();
		})
	});
});

describe("create " + metrics[1].name, function(){
	it("should without error", function(done){
		Metric.create(metrics[1], function (err){
			assert(!err || err == "ER_METRIC_EXIST");
			done();
		})
	});
});


describe("create " + metrics[2].name, function(){
	it("should without error", function(done){
		Metric.create(metrics[2], function (err){
			assert(!err || err == "ER_METRIC_EXIST");
			done();
		})
	});
});

describe("create " + metrics[3].name, function(){
	it("should without error", function(done){
		Metric.create(metrics[3], function (err){
			assert(!err || err == "ER_METRIC_EXIST");
			done();
		})
	});
});

