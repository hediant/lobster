var LRU = require('lru-cache');
var moment = require('moment');

function TopicCache(capacity){
	var options_ = {
		max : capacity,
		dispose : function (key, val){
			val.close();
		}
	}

	LRU.call(this, options_);
	var me = this;

	// cycle timer
	var timer_ = null;

	// clear handles of each topic
	var clearHandles_ = function (){
		// remove 5 days before
		var ts = moment() - moment.duration(5, "days");
		me.forEach(function (val, key, cache){
			val.clearHandles(ts);
		});
	}

	this.cycle = function (){
		timer_ = setInterval(clearHandles_, moment.duration(1, "days").valueOf());
	}

	this.close = function (){
		if (timer_)
			clearInterval(timer_);
	}

	// begin cycle clear
	this.cycle();
}
require('util').inherits(TopicCache, LRU);

module.exports = TopicCache;