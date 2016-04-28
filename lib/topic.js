var fs = require('fs');
var path = require('path');
var config = require('../config.json');
var moment = require('moment');
var EventEmitter = require('events').EventEmitter;
var Q = require("q");

function Topic (topic_name){
	EventEmitter.call(this);
	var me = this;
	var is_ready_ = false;
	var last_date_ = moment().format("YYYYMMDD");

	var base_path_ = path.join(config.db_path, topic_name);
	var metric_setting_ = path.join(base_path_, "metric");
	
	var metric_ = null;
	var metric_path_ = null;
	var log_file_path_ = null;

	this.getTopicName = function (){
		return topic_name;
	}

	this.getMetric = function (){
		return metric_;
	}

	this.isReady = function (){
		return is_ready_;
	}

	this.getHeadLine = function (){
		var keys = ["__ts__"];
		keys = keys.concat(metric_.keys.map(function (it){
			return it["name"];
		}));
		keys.push("\n");

		return keys.join(config.split);
	}

	this.reset = function (){
		metric_ = null;
		metric_path_ = null;
		log_file_path_ = null;
	}

	this.getWriteStream = function (metric, cb){
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

		Q.fcall(function (){
			// init metric path
			return Q.Promise(function (resolve, reject){
				if (!metric_ || metric_.id != metric.id){
					me.reset();
				}

				if (metric_path_)
					resolve();
				else{
					var mp = path.join(base_path_, metric.id.toString());
					createMetricPath(mp, function (err){
						if (err)
							reject(err);
						else{
							metric_path_ = mp;
							resolve();
						}
					})
				}
			})
		}).then(function (){
			// write metric settings
			return Q.Promise(function (resolve, reject){
				if (metric_)
					return resolve();

				saveMetricSettings(metric, function (err){
					if (err)
						reject(err);
					else{
						metric_ = metric;
						resolve();
					}
				})
			})
		}).then(function (){
			// init log file 
			return Q.Promise(function (resolve, reject){
				
				var log_file = getLogFilePath(metric_);
				var cur_date = moment().format("YYYYMMDD");
				if (last_date_ < cur_date){
					log_file_path_ = null;
				}

				var wstream = function (need_head_line){
					createWriteStream(log_file, need_head_line, function (err, stream){
						if (!err){
							log_file_path_ = log_file;
							last_date_ = cur_date;

							resolve();
							cb && cb(null, stream);
						}
						else{
							reject(err);
						}
					});						
				}

				if (!log_file_path_){
					fs.exists(log_file, function (exists){
						wstream(!exists);
					});
				}
				else{
					wstream();
				}

			})
		}).catch (function (err){
			if (err.code == "ENOENT"){
				is_ready_ = false;
				me.getWriteStream(cb);
			}
			else{
				cb && cb(err);
			}
		});
	}

	var getLogFilePath = function (metric){
		var cur_date = moment().format("YYYYMMDD");
		var log_file_name = cur_date + "." + metric.ver + ".log";
		var log_file = path.join(metric_path_, log_file_name);

		return log_file;
	}

	var createMetricPath = function (metric_path, cb){
		// create metric path if need		
		fs.exists(metric_path, function (exists){
			if (exists){
				cb();
			}
			else{
				fs.mkdir(metric_path, function (err){
					if (err){
						cb(err);
					}
					else{
						cb();
					}
				});
			}
		});
	}

	var saveMetricSettings = function (metric, cb){
		fs.writeFile(metric_setting_, metric.id.toString(), cb);
	}

	var createWriteStream = function (log_file, need_head_line, cb){
		var wstream = fs.createWriteStream(log_file, {"flags":"a"});
		wstream.once('open', function (){
			// write head line if needed
			if (need_head_line){
				wstream.write(me.getHeadLine());
			}

			// callback
			cb(null, wstream);
		});

		wstream.once('error', function (err){
			cb(err);
		});
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
						if (err)
							reject(err);
						else
							resolve();
					});
				}
			});
		});		
	}

	this.init = function (){
		Q
		.fcall(initBasePath)
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

module.exports = Topic;