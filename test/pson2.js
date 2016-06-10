var PSON = require("pson");
var pson = new PSON.StaticPair();

var tag_count = 200;
var system_count = 10000;

var generate = function (){
	var ret = {};
	for (var i=0; i<tag_count; i++){
		ret['tag_'+ (i+1).toString()] = Math.floor(Math.random()*1000000);
	}

	return ret;
}

var testJSONEncoding = function (){
	var data = generate();
	var t1 = Date.now();
	for (var i=0; i<system_count; i++){
		JSON.stringify(data);
	}
	var t2 = Date.now();
	return t2 -t1;
}

var testJSONDencoding = function (){
	var data = generate();
	var encoded = JSON.stringify(data);

	var t1 = Date.now();
	for (var i=0; i<system_count; i++){
		JSON.parse(encoded);
	}
	var t2 = Date.now();
	return t2 - t1;
}

var testPSONEncoding = function (){
	var data = generate();
	var t1 = Date.now();
	for (var i=0; i<system_count; i++){
		pson.encode(data);
	}
	var t2 = Date.now();
	return t2 -t1;	
}

var testPSONDencoding = function (){
	var data = generate();
	var encoded = pson.encode(data).toBuffer();

	var t1 = Date.now();
	for (var i=0; i<system_count; i++){
		pson.decode(encoded);
	}
	var t2 = Date.now();
	return t2 - t1;
}

var test = function (){
	var json_encode = testJSONEncoding();
	console.log("JSON encode: %s ms", json_encode);

	var json_dencode = testJSONDencoding();
	console.log("JSON dencode: %s ms", json_dencode);

	var pson_encode = testPSONEncoding();
	console.log("PSON encode: %s ms", pson_encode);

	var pson_dencode = testPSONDencoding();
	console.log("PSON dencode: %s ms", pson_dencode);	

	console.log("PSON/JSON encode: %s", (json_encode/pson_encode).toFixed(2));
	console.log("PSON/JSON dencode: %s", (json_dencode/pson_dencode).toFixed(2));
}

test();