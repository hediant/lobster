var EventEmitter = require('events').EventEmitter
	, path = require('path')
	, config = require('../config.json')
	, moment = require('moment')
	, fs = require('fs')
	, Q = require('q')
	, Metric = require('./metric')
	, csv = require('fast-csv')
	, sortBy = require('sort-array')
	, Topic = require('./topic');

function Reader(topic_name, keys){
	EventEmitter.call(this);

	var me = this;
	var base_path_ = Topic.BasePath(topic_name);
	var metric_setting_ = Topic.MetricSettingPath(topic_name);

	// @handle - string, file path
	// @metric - object of Metric	
	// @where - function (row)
	// @cb - function (err, data)
	var readOneHandle = function (handle, metric, where, cb){

	}

	var readHandles = function (handles, metric, where, cb){
		cb && cb(null, {});
	}

	// @cb - function (err, metric)
	var getMetric = function (cb){
		fs.readFile(metric_setting_, function (err, data){
			if (err){
				if (err.code == "ENOENT")
					cb("ER_TOPIC_NOT_EXIST");
				else
					cb("ER_IO_ERROR");
				return;
			}

			var metric_id = data.toString();
			Metric.get(metric_id, function (err, metric){
				cb (err, metric);
			});
		});
	}

	// Get match handles by days
	// @days - array of string, "YYYYMMDD"
	// @metric_id
	// @cb - function (err, handles)
	var getHandles = function (days, metric_id, cb){
		fs.readdir(Topic.MetricPath(topic_name, metric_id), function (err, files){
			if (err){
				if (err.code == "ENOENT")
					cb("ER_TOPIC_NOT_EXIST");
				else
					cb("ER_IO_ERROR");
				return;				
			}

			cb(null, files.filter(function (file){
				return days.indexOf(file.substr(0, 8)) != -1;
			}));
		})
	}

	// @start - timestamp, number
	// @end - timestamp, number
	// assert start <= end, duration <= 7 days
	var getDayRanges = function (start, end){
		var sday_ = moment(start).format("YYYYMMDD");
		var eday_ = moment(end).format("YYYYMMDD");

		var days = [];
		var oneDay = moment.duration(1, "days");
		while(sday_ <= eday_){
			days.push(sday_);
			sday_ = moment(moment(sday_) + oneDay).format("YYYYMMDD");
		}

		return days;
	}

	var mergeResults = function (results, options){
		return {};
	}

	var readRawSeries = function (start, end, options){
		var metric, handles;
		Q.fcall(function (){
			// get metric
			return Q.Promise(function (resolve, reject){
				getMetric(function (err, results){
					if (err){
						if (err == "ER_METRIC_NOT_EXIST")
							reject("ER_NO_DATA");
						else
							reject(err);
					}
					else{
						metric = results;
						resolve();
					}
				})
			})
		}).then(function (){
			// get handles
			return Q.Promise(function (resolve, reject){
				var days = getDayRanges(start, end);
				getHandles(days, metric.id, function (err, results){
					if (err)
						reject(err);
					else{
						handles = results;
						resolve();
					}
				})
			})
		}).then(function (){
			// read handles
			return Q.Promise(function (resolve, reject){
				readHandles(handles, metric, options.where, function (err, results){
					if (err)
						reject(err);
					else{
						me.emit('data', mergeResults(results, options));
						resolve();
					}
				})
			})
		}).catch (function (err){
			me.emit('error', err);
		});
	}

	this.readRaw = function (start, end, options){
		var start_ = start, end_ = end ? end : Date.now();
		var options_ = {
			"limit" : 1500,
			"forward" : true,
			"where" : function (row){
				return true;
			}
		};

		if (options && options.limit){
			options_.limit = parseInt(options.limit) < 1500 ? parseInt(options.limit) : 1500;
		}
		if (options && typeof options.forward != undefined){
			options_.forward = options.forward ? true : false
		}
		if (options && typeof options.where == "function"){
			options_.where = options.where;
		}

		// set forward options
		if (end_ < start_){
			options_.forward = false;
			var t = end_; end_ = start_; start_ = t;
		}

		// The maximum time range of a query is 1 weeks
		var a_week = moment.duration(1, "weeks").valueOf();
		if ((end_ - start_) > a_week){
			if (options_.forward)
				end_ = start_ + a_week;
			else
				start_ = end_ - a_week;
		}

		// do query series
		readRawSeries(start_, end_, options_);
	}

}
require('util').inherits(Reader, EventEmitter);

module.exports = Reader;

