var mr = require('murmurhash-js')
	, config = require('../config')
	, seed = 0;

module.exports = function (topic_name){
	return mr.murmur3(topic_name, seed) % config.regions;
}