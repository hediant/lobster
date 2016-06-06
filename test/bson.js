var bson = require("bson");
var BSON = new bson.BSONPure.BSON();
var csv = require('csv-parser');
var fs = require('fs');
var config = require('../config.json');

var file = "/home/lobster/data/system_1/3/20160516.0.log";

var rows = [];

fs.createReadStream(file)
.pipe(csv({
	"separator" : config.separator
}))
.on('data', function (data){
	var ret = {};
	for (var key in data){
		ret[key] = Number(data[key]);
	}

	rows.push(ret);
})
.on('end', function (){

	var t1 = Date.now();
	var chunk = BSON.serialize(rows, false, true, false);
	var t2 = Date.now();
	console.log("Serialize object, size:%s MB, cost:%s ms.", Math.round(chunk.length / (1024 * 1024)) , (t2 - t1));
	console.log(chunk);

	t1 = Date.now();
	var data = BSON.deserialize(chunk);
	t2 = Date.now();
	console.log("Deserialize %s MB chunk, cost:%s ms.", Math.round(chunk.length / (1024 * 1024)), (t2 - t1));
	//console.log(data)

	var events = [];
	rows.forEach(function (row){
		var ev = [];
		for (var key in row){
			ev.push(row[key])
		}
		events.push(ev);
	});

	t1 = Date.now();
	var context = BSON.serialize(events, false, true, false);
	t2 = Date.now();
	console.log("Serialize %s events (200 fields per event), cost: %s ms.", events.length, (t2 - t1));
	console.log(context);

	t1 = Date.now();
	decode_context = BSON.deserialize(context);
	t2 = Date.now();
	console.log("Deserialize %s events (200 fields per event), size: %sMB, cost: %s ms.", 
		events.length,
		Math.round(context.length / (1024 * 1024)),
		(t2 - t1));
	//console.log(decode_context);		
})