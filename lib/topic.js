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

	// openning handles
	var handles_ = {};

	this.addHandle = function (handle){
		handles_[handle] = Date.now();
	}

	this.hasHandle = function (handle){
		return handles_[handle];
	}

	this.getHandles = function (){
		return handles_;
	}

	this.clearHandles = function (ts){
		for (var handle in handles_){
			if (handles_[handle] < ts){
				delete handles_[handle];
			}
		}
	}

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
		handles_ = {};
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