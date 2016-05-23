var config = require('../config');
var moment = require('moment');

module.exports = function (data, timestamp, metric){
	var rt = new Array(metric.keys.length + 1);
	rt[0] = moment(timestamp).format("HHmmss");

	metric.keys.forEach(function (key, i){
		rt[i+1] = data.hasOwnProperty(key.name) ? data[key.name] : null;
	});

	return rt.join(config.separator) + "\n";
}