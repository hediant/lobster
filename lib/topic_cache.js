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

	this.close = function (){
		me.forEach(function (value, key, cache){
			value.close();
		});
	}
}
require('util').inherits(TopicCache, LRU);

module.exports = TopicCache;
