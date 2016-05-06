var Reader = require('../lib/reader');
var moment = require('moment');

var reader = new Reader("system_100", ["tag_1", "tag_2"]);
debugger
reader.readRaw(moment()-moment.duration(2, "days"), moment().valueOf());
reader.on('data', function (data){
	console.log(data);
});
reader.on('error', function (err){
	console.log(err);
})