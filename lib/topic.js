var fs = require('fs')
	, path = require('path')
	, config = require('../config.json')
	, moment = require('moment')
	, EventEmitter = require('events').EventEmitter
	, Q = require("q");

function Topic (topic_name){
	EventEmitter.call(this);
	var me = this;
	var is_ready_ = false;
	var last_date_ = moment().format("YYYYMMDD");

	var base_path_ = Topic.BasePath(topic_name);
	var metric_setting_ = Topic.MetricSettingPath(topic_name);
	
	var metric_ = null;
	var metric_path_ = null;
	var log_file_path_ = null;

	var write_stream_lock_ = false;
	var read_stream_lock_ = false;

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

		return keys.join(config.delimiter);
	}

	this.reset = function (){
		metric_ = null;
		metric_path_ = null;
		log_file_path_ = null;
	}

	var getLogFilePath = function (metric){
		var cur_date = moment().format("YYYYMMDD");
		return Topic.LogFilePath(topic_name, cur_date, metric.id, metric.ver);
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

	this.getWriteStream = function (metric, cb){
		if (!is_ready_){
			me.init();
			me.once('ready', function (){
				me.getWriteStream(metric, cb);
			});
			me.once('error', function (err){
				cb && cb(err);
			});

			return;
		}

		//
		// Process of obtaining write_fs_stream must be transactional,
		// as it will modify metric_ internal state variables in the process of acquisition.
		//
		// Thus, concurrent performance of this process will decrease.
		// Fortunately, we rarely change a Topic's Metric.
		//
		// Without changing of Topic's Metric, the impact of the transaction on performance can be ignored.
		//
		if (write_stream_lock_){
			me.once('write_stream_drain', function (){
				me.getWriteStream(metric, cb);
			});

			return;
		}

		// set status
		write_stream_lock_ = true;

		Q.fcall(function (){
			// init metric path
			return Q.Promise(function (resolve, reject){
				if (!metric_ || metric_.id != metric.id){
					me.reset();
				}

				if (metric_path_)
					resolve();
				else{
					var mp = Topic.MetricPath(topic_name, metric.id);
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

							write_stream_lock_ = false;
							me.emit("write_stream_drain");
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

				write_stream_lock_ = false;
				me.emit("write_stream_drain");
			}
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

Topic.BasePath = function (topic_name){
	return path.join(config.db_path, topic_name);
}

Topic.LogFilePath = function (topic_name, day, metric_id, metric_ver){
	var log_file_name = day + "." + metric_ver + ".log";
	return path.join(Topic.MetricPath(topic_name, metric_id), log_file_name);	
}

Topic.MetricPath = function (topic_name, metric_id){
	return path.join(Topic.BasePath(topic_name), metric_id.toString());
}

Topic.MetricSettingPath = function (topic_name){
	return path.join(Topic.BasePath(topic_name), "metric");
}

module.exports = Topic;