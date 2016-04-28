var fs = require('fs');
var path = require('path');
var config = require('../config.json');
var moment = require('moment');
var EventEmitter = require('events').EventEmitter;
var Q = require("q");

function Topic (topic_name, metric){
	EventEmitter.call(this);
	var me = this;
	var is_ready_ = false;
	var need_head_line_ = false;
	var last_date_ = moment().format("YYYYMMDD");

	var base_path_ = path.join(config.db_path, topic_name);
	var metric_path_ = path.join(base_path_, metric.id);
	var metric_setting_ = path.join(base_path_, "metric");

	this.getTopicName = function (){
		return topic_name;
	}

	this.getMetric = function (){
		return metric;
	}

	this.isReady = function (){
		return is_ready_;
	}

	this.getHeadLine = function (){
		var keys = ["__ts__"];
		keys = keys.concat(metric.keys.map(function (it){
			return it["name"];
		}));
		keys.push("\n");

		return keys.join(config.split);
	}

	this.getWriteStream = function (cb){
		if (!is_ready_){
			me.init();
			me.once('ready', function (){
				me.getWriteStream(cb);
			});
			me.once('error', function (err){
				cb && cb(err);
			});

			return;
		}

		var log_file = getLogFilePath();
		var cur_date = moment().format("YYYYMMDD");
		if (last_date_ < cur_date)
			need_head_line_ = true;

		var wstream = fs.createWriteStream(log_file, {"flags":"a"});
		wstream.once('open', function (){
			// set last date
			last_date_ = cur_date;

			// write head line if needed
			if (need_head_line_){
				wstream.write(me.getHeadLine());
				need_head_line_ = false;
			}

			// callback
			cb && cb(null, wstream);
		});

		wstream.once('error', function (err){
			if (err.code == "ENOENT"){
				is_ready_ = false;
				me.getWriteStream(cb);
			}
			else{
				cb && cb(err);
			}
		})
	}

	var getLogFilePath = function (){
		var cur_date = moment().format("YYYYMMDD");
		var log_file_name = cur_date + "." + metric.ver + ".log";
		var log_file = path.join(metric_path_, log_file_name);

		return log_file;		
	}

	var initBasePath = function (){
		// create base path if need
		return Q.Promise(function (resolve, reject){
			fs.exists(base_path_, function (exists){
				if (exists){
					resolve();
				}
				else{
					fs.mkdir(base_path_, function (err){
						need_head_line_ = true;
						if (err)
							reject(err);
						else
							resolve();
					});
				}
			});
		});		
	}

	var initMetricPath = function (){
		// create metric path if need
		return Q.Promise(function (resolve, reject){
			fs.exists(metric_path_, function (exists){
				if (exists)
					resolve();
				else{
					fs.mkdir(metric_path_, function (err){
						need_head_line_ = true;
						if (err)
							reject(err);
						else
							resolve();
					});
				}
			});
		});
	}

	var saveMetricSettings = function (){
		return Q.Promise(function (resolve, reject){
			fs.writeFile(metric_setting_, metric.id.toString(), function (err){
				if (err)
					reject(err);
				else
					resolve();
			});
		});
	}

	var needHeadLine = function (){
		return Q.Promise(function (resolve, reject){
			fs.exists(getLogFilePath(), function (exists){
				if (!exists)
					need_head_line_ = true;
				resolve();
			});
		});
	}

	this.init = function (){
		Q
		.fcall(initBasePath)
		.then(saveMetricSettings)
		.then(initMetricPath)
		.then(needHeadLine)
		.then(function (){
			is_ready_ = true;
			me.emit('ready');
		})
		.catch(function (err){
			me.emit('error', err);
		});
	}

	// do init
	this.init();
}
require('util').inherits(Topic, EventEmitter);