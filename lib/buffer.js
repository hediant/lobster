var moment = require('moment');

function Buffer(){
	var me = this;
	var buffer_ = {};
	var keywords_ =[];
	var len_ = 0;

	this.size = function (){
		return len_;
	}

	var toKey_ = function (topic_name, day){
		return topic_name + "." + day;
	}

	var parseKey_ = function (key){
		return {
			"topic" : key.substr(0, key.length - 9),
			"day" : key.substr(-8, 8)
		}
	}

	this.push = function (topic_name, data, metric){
		var ts = new Date();
		var day = moment(ts).format("YYYYMMDD");
		var key = toKey_(topic_name, day);

		var val = buffer_[key];
		if (!val){
			buffer_[key] = [];
			val = buffer_[key];
			keywords_.push(key);
		}

		val.push({
			"data" : data,
			"metric" : metric,
			"ts" : ts
		});

		len_ ++;
	}

	this.shift = function (){
		var key = keywords_.shift();
		if (!key)
			return null;

		var meta = parseKey_(key);
		var series = buffer_[key];

		delete buffer_[key];
		len_ = len_ - series.length;

		return {
			"topic" : meta.topic,
			"day" : meta.day,
			"series" : series
		}
	}

	this.clear = function (){
		buffer_ = {};
		keywords_ = [];
		len_ = 0;
	}
}

module.exports = Buffer;