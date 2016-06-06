var fs = require('fs')
	, path = require('path')
	, config = require('../config.json')
	, moment = require('moment')
	, EventEmitter = require('events').EventEmitter
	, Q = require("q")
	, LBF = require('./lbf');

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

	// current openning stream
	var openning_stream_ = null;
	var openning_logfile_path_ = null;

	// mutexs
	var write_stream_lock_ = false;

	this.getTopicName = function (){
		return topic_name;
	}

	this.getMetric = function (){
		return metric_;
	}

	var createMetricPath_ = function (metric_path, cb){
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

	var createFlag_ = function (metric){
		return LBF.flag(metric.ver).toBuffer();
	}

	var createWriteStream_ = function (log_file, flag, cb){
		var wstream = fs.createWriteStream(log_file, {"flags":"a"});
		wstream.once('open', function (){
			// write head line if needed
			if (flag){
				wstream.write(flag);
			}

			// callback
			cb(null, wstream);
		});

		wstream.once('error', function (err){
			cb(err);
		});
	}

	var is_current_openning_ = function (log_file){
		return openning_logfile_path_ == log_file;
	}

	var close_openning_stream_ = function (){
		if (openning_stream_){
			// close stream
			openning_stream_.end();

			// reset
			openning_stream_ = null;
			openning_logfile_path_ = null;
		}
	}

	this.getWriteStream = function (day, metric, cb){
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
				me.getWriteStream(day, metric, cb);
			});

			return;
		}

		// set status
		write_stream_lock_ = true;		

		// log file fullpath(handle)
		var log_file = Topic.LogFilePath(topic_name,
			day,
			metric.id,
			metric.ver);

		// if stream is openning
		if (is_current_openning_(log_file)){
			cb && cb(null, openning_stream_);

			write_stream_lock_ = false;
			me.emit("write_stream_drain");

			return;		
		}
		else{
			close_openning_stream_();
		}

		// init handle and cache it
		Q.fcall(function (){
			// create metric path && write metric settings
			return Q.Promise(function (resolve, reject){
				var old_metric = me.getMetric();
				if (old_metric && old_metric.id == metric.old){
					return resolve();
				}
				
				me.setMetric(metric, function (err){
					err ? reject(err) : resolve();
				});
			})
		}).then(function (){
			// init log file 
			return Q.Promise(function (resolve, reject){
				var wstream = function (flag){
					createWriteStream_(log_file, flag, function (err, stream){
						if (!err){
							// set openning stream
							openning_logfile_path_ = log_file;
							openning_stream_ = stream;

							cb && cb(null, stream);

							// clear mutex
							write_stream_lock_ = false;
							me.emit("write_stream_drain");

							resolve();
						}
						else{
							reject(err);
						}
					});						
				}

				fs.exists(log_file, function (exists){
					wstream(exists ? null : createFlag_(metric));
				});

			})
		}).catch (function (err){
			cb && cb(err);

			write_stream_lock_ = false;
			me.emit("write_stream_drain");
		});		
	}

	this.setMetric = function (metric, cb){
		// create metric path && write metric settings
		Q.fcall(function (){
			// create metric path
			return Q.Promise(function (resolve, reject){
				var mp = Topic.MetricPath(topic_name, metric.id);
				createMetricPath_(mp, function (err){
					if (err)
						reject("ER_MAKE_METRIC_DIR");
					else
						resolve();
				})
			});
		}).then(function (){
			// write metric settings
			return Q.Promise(function (resolve, reject){
				var msp = Topic.MetricSettingPath(topic_name);
				fs.writeFile(msp, metric.id.toString(), function (err){
					if (err)
						reject("ER_SAVE_METRIC_SETTINGS");
					else
						resolve();
				});
			});
		}).then(function (){
			metric_ = metric;
			cb && cb();
		}).catch (function (err){
			cb && cb(err);
		});
	}

	this.isReady = function (){
		return is_ready_;
	}

	this.reset = function (){
		metric_ = null;
		metric_path_ = null;
		log_file_path_ = null;
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

	this.close = function (){
		// close current openning stream
		close_openning_stream_();
		// reset
		me.reset();
		me.removeAllListeners();
	}

	// do init
	this.init();
}
require('util').inherits(Topic, EventEmitter);

Topic.BasePath = function (topic_name){
	return path.join(config.db_path, topic_name);
}

Topic.LogFileName = function (day, metric_ver){
	return day + "." + metric_ver + ".lbf";
}

Topic.LogFilePath = function (topic_name, day, metric_id, metric_ver){
	return path.join(Topic.MetricPath(topic_name, metric_id), Topic.LogFileName(day, metric_ver));	
}

Topic.MetricPath = function (topic_name, metric_id){
	return path.join(Topic.BasePath(topic_name), metric_id.toString());
}

Topic.MetricSettingPath = function (topic_name){
	return path.join(Topic.BasePath(topic_name), "metric");
}

module.exports = Topic;
