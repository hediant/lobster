var moment = require('moment')
	, Topic = require('./topic')
	, encape = require('./encape');

function Buffer(){
	var me = this;
	var buffer_ = {};
	var keywords_ =[];
	var len_ = 0;

	this.size = function (){
		return len_;
	}

	var toKey_ = function (topic_name, day, metric){
		return topic_name + "/" + metric.id + "/" + Topic.LogFileName(day, metric.ver);
	}

	this.push = function (topic_name, data, metric, ts){
		var timestamp = ts || Date.now();
		var day = moment(timestamp).format("YYYYMMDD");
		var key = toKey_(topic_name, day, metric);

		var val = buffer_[key];
		if (!val){
			buffer_[key] = {
				"topic" : topic_name,
				"day" : day,
				"metric" : metric,
				"series" : [],
				"length" : 0
			};

			val = buffer_[key];
			keywords_.push(key);
		}

		val.series.push(encape(data, timestamp, metric));
		val.length ++;

		len_ ++;
	}

	this.shift = function (){
		var key = keywords_.shift();
		if (!key)
			return null;

		var ret = buffer_[key];

		delete buffer_[key];
		len_ = len_ - ret.length;
		
		return ret;
	}

	this.clear = function (){
		buffer_ = {};
		keywords_ = [];
		len_ = 0;
	}
}

module.exports = Buffer;