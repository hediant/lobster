var EventEmitter = require('events').EventEmitter
	, path = require('path')
	, config = require('../config.json')
	, moment = require('moment')
	, fs = require('fs')
	, Q = require('q')
	, Metric = require('./metric')
	, csv = require('fast-csv')
	, sortBy = require('sort-array');

function Reader(topic_name, keys){
	EventEmitter.call(this);

	var me = this;
	var base_path_ = path.join(config.db_path, topic_name);
	var metric_setting_ = path.join(base_path_, "metric");

	// @day - string, "YYYYMMDD"
	// @cb - function (err, fstream)
	this.getReadStream = function (day, cb){
		
	}

}
require('util').inherits(Reader, EventEmitter);

module.exports = Reader;

