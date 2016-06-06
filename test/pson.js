var PSON = require("pson");
var csv = require('csv-parser');
var fs = require('fs');
var config = require('../config.json');
var Long = require('long');

var file = "/home/lobster/data/system_1/3/20160516.0.log";

var rows = [];

var t1 = Date.now(), t2;

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
	t2 = Date.now();
	console.log("Load CSV File, 2MB, cost:%s ms.", (t2 - t1));

	console.log("");
	var pson = new PSON.ProgressivePair();
	t1 = Date.now();
	var chunk = pson.encode(rows);
	t2 = Date.now();
	console.log("Serialize object, size:%s MB, cost:%s ms.", Math.round(chunk.buffer.length / (1024 * 1024)) , (t2 - t1));
	console.log(chunk);

	console.log("");
	t1 = Date.now();
	var data = pson.decode(chunk.buffer);
	t2 = Date.now();
	console.log("Deserialize %s MB chunk, cost:%s ms.", Math.round(chunk.buffer.length / (1024 * 1024)), (t2 - t1));
	console.log(data.length);

	console.log("");
	t1 = Date.now();
	var events = [];
	rows.forEach(function (row){
		var ev = [];
		for (var key in row){
			ev.push(row[key])
		}
		events.push(ev);
	});
	t2 = Date.now();
	console.log("Iterate %s events(200 fields per event), cost:%s ms.", events.length, (t2 - t1));

	console.log("");
	pson = new PSON.ProgressivePair();
	t1 = Date.now();
	var context = pson.encode(events);
	t2 = Date.now();
	console.log("Serialize %s events (200 fields per event), cost: %s ms.", events.length, (t2 - t1));
	console.log(context);

	console.log("");
	t1 = Date.now();
	decode_context = pson.decode(context);
	t2 = Date.now();
	console.log("Deserialize %s events (200 fields per event), size: %sMB, cost: %s ms.", 
		events.length,
		Math.round(context.buffer.length / (1024 * 1024)),
		(t2 - t1));
	//console.log(decode_context);	

	var ck = [
		0,
		1,
		"abc",
		NaN,
		undefined, 
		{"x":1, "y":2}, 
		null, 
		null, 
		new Date(), 
		Date.now(),
		Long.fromNumber(Date.now()),
		Long.fromNumber(100)
	];

	console.log("");
	console.log(ck);
	var encode_ck = pson.encode(ck);
	console.log(encode_ck);
	var decode_ck = pson.decode(encode_ck.buffer);
	console.log(decode_ck);


	console.log("");
	t1 = Date.now();
	var buf = [];
	events.forEach(function (ev){
		buf.push(pson.encode(ev).buffer);
	});
	t2 = Date.now();
	console.log("Encape events(%s, %s), cost:%s ms.",
		events.length,
		events.length * 200,
		(t2 - t1));

	console.log("");
	t1 = Date.now();
	var decode_events = new Array(buf.length);
	buf.forEach(function (chunk, i){
		decode_events[i] = pson.decode(chunk);
	});
	console.log(buf[0])
	console.log(buf[1])

	t2 = Date.now();
	console.log("Dencape events(%s, %s), cost:%s ms.",
		decode_events.length,
		decode_events.length * 200,
		(t2 - t1));	
	//console.log(decode_events[0]);
})