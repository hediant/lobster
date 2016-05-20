var config = require('../config.json')
	, Buffer = require('./buffer')
	, Q = require('q')
	, moment = require('moment')
	, Topic = require('./topic')
	, TopicCache = require('./topic_cache')
	, EventEmitter = require('events').EventEmitter
	, fs = require('fs');

function Writer(){
	EventEmitter.call(this);
	var me = this;

	// set max listener to match config.concurrent_write
	me.setMaxListeners(config.concurrent_write * 2);

	// write buffer
	var buffer_ = new Buffer();
	var timer_ = null;

	// cached topics
	var topics_ = new TopicCache(config.topic_cache);

	// mutexs
	var buffer_write_lock_ = false;
	var write_stream_lock_ = false;

	var getHeadLine_ = function (metric){
		var keys = ["__ts__"];
		keys = keys.concat(metric.keys.map(function (it){
			return it["name"];
		}));
		keys.push("\n");

		return keys.join(config.delimiter);
	}

	var createWriteStream_ = function (log_file, head_line, cb){
		var wstream = fs.createWriteStream(log_file, {"flags":"a"});
		wstream.once('open', function (){
			// write head line if needed
			if (head_line){
				wstream.write(head_line);
			}

			// callback
			cb(null, wstream);
		});

		wstream.once('error', function (err){
			cb(err);
		});
	}

	this.createWriteStream = function (topic_name, day, metric, cb){
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
				me.createWriteStream(topic_name, day, metric, cb);
			});

			return;
		}

		// log file fullpath(handle)
		var log_file = Topic.LogFilePath(topic_name,
			day,
			metric.id,
			metric.ver);

		// if topic and handle cached
		var topic = topics_.get(topic_name);
		if (topic){
			if (!topic.getMetric() || topic.getMetric().id != metric.id){
				// we need reset topic metric later
			}
			else if (topic.hasHandle(log_file)){
				createWriteStream_(log_file, null, function (err, stream){
					write_stream_lock_ = false;
					me.emit("write_stream_drain");
					cb && cb(err, stream);
				});

				return;				
			}
		}

		// set status
		write_stream_lock_ = true;

		// init handle and cache it
		Q.fcall(function (){
			// create and cache topic if need
			return Q.Promise(function (resolve, reject){
				if (topic)
					return resolve();

				topic = new Topic(topic_name);
				topic.once('ready', function (){
					topics_.set(topic_name, topic);
					resolve();
				});
				topic.once('error', function (err){
					reject("ER_CACHE_TOPIC");
				});
			})
		}).then(function (){
			// create metric path && write metric settings
			return Q.Promise(function (resolve, reject){
				var old_metric = topic.getMetric();
				if (old_metric && old_metric.id == metric.old){
					return resolve();
				}
				
				topic.setMetric(metric, function (err){
					err ? reject(err) : resolve();
				});
			})
		}).then(function (){
			// init log file 
			return Q.Promise(function (resolve, reject){
				var wstream = function (head_line){
					createWriteStream_(log_file, head_line, function (err, stream){
						if (!err){
							// add handle to cache
							topic.addHandle(log_file);

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
					wstream(exists ? null : getHeadLine_(metric));
				});

			})
		}).catch (function (err){
			cb && cb(err);

			write_stream_lock_ = false;
			me.emit("write_stream_drain");
		});		
	}

	this.append = function (topic_name, data, metric){
		if (buffer_.size() >= config.max_write_buf)
			return -1;

		buffer_.push(topic_name, data, metric);

		if (buffer_.size() >= config.flush_water_level)
			me.writeBuffer("flush_water_level");

		return 0;
	}

	this.writeBuffer = function (reason){
		if (buffer_write_lock_)
			return;

		if (config.debug && config.diagnosis){
			console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("Begin write buffer, size:%s, reason:%s.", buffer_.size(), reason);
		}

		buffer_write_lock_ = true;

		// diagnosis
		var start_write_buf = Date.now();
		var total_write_buffer_size = 0;
		var total_write_topic_size = 0;
		var write_buf_size = 0;
		var write_topic_size = 0;
		var end_write_buf;
		var timer = null;

		var showProgress_ = function (){
			if (!timer){
				timer = setInterval(function (){
					console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
					console.log("Write Buffer, size:%s, topics:%s, leave:%s.",
						write_buf_size,
						write_topic_size,
						buffer_.size());

					write_buf_size = 0;
					write_topic_size = 0;

				}, 1 * 60 * 1000);
			}
		}

		if (config.diagnosis){
			showProgress_();
		}

		// do write parallel		
		var co_write = 0;
		var write_ = function (){
			while(co_write < config.concurrent_write && buffer_.size()){
				/*
				// for debug
				if (config.debug)
					console.log("write buffer:%s, co_write:%s, is writting:%s.", 
						buffer_.size(), 
						co_write,
						buffer_write_lock_);
				*/

				(function (doc, complete){
					co_write ++;

					// diagnosis
					total_write_topic_size ++;
					total_write_buffer_size += doc.series.length;

					write_buf_size ++;
					write_topic_size += doc.series.length;

					// flush to disk
					me.flush(doc, function (err){
						if (err && config.debug)
							console.error(err);

						co_write --;

						if (complete){
							end_write_buf = Date.now();

							if (config.diagnosis){
								console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
								console.log("Flush completed, size:%s, topics:%s, cost:%sms.",
									total_write_buffer_size,
									total_write_topic_size,
									(end_write_buf - start_write_buf));
							}

							// clear timer
							if (timer){
								clearInterval(timer);
								timer = null;
							}

							// clear mutex
							buffer_write_lock_ = false;
						}
					});
				})(buffer_.shift(), !buffer_.size());

			}

			if (buffer_.size()){
				setImmediate(write_);
			}
		}

		write_();
	}

	this.encape = function (data, timestamp, metric){
		var rt = new Array(metric.keys.length + 1);
		rt[0] = moment(timestamp).format("HHmmss");

		metric.keys.forEach(function (key, i){
			rt[i+1] = data.hasOwnProperty(key.name) ? data[key.name] : null;
		});

		return rt.join(config.delimiter) + "\n";
	}

	this.flush = function (doc, cb){
		var metric_count = 0;
		var queue = {};
		var metrics = [];

		var topic_name = doc.topic;
		var day = doc.day;

		// sort by metric id
		doc.series.forEach(function (it){
			var the_series = queue[it.metric.id];
			if (!the_series){
				queue[it.metric.id] = [];
				the_series = queue[it.metric.id];
				metrics.push(it.metric);
			}

			the_series.push(me.encape(it.data, it.ts, it.metric));
		});

		var flush_ = function (metric, series){
			return Q.Promise(function (resolve, reject){
				me.createWriteStream(topic_name, day, metric, function (err, fstream){
					if (err)
						reject(err);
					else{
						var i = 0, count = series.length;
						var write_ = function (){
							var ok = true;
							while(i<count && ok) {
								/*
									// for debug
									console.log("metric:",metric.id)
									console.log(fstream.path)
									console.log(series[i])
								*/
								ok = fstream.write(series[i++]);
							};

							if (i<count)
								fstream.once('drain', write_);
							else
								fstream.end();
						};

						fstream.once('finish', function (){
							resolve();
						});

						write_();
					}
				})
			})
		}

		var callbacks = [];
		metrics.forEach(function (metric){
			callbacks.push(flush_(metric, queue[metric.id]));
		});

		Q.all(callbacks)
		.then(function (results){
			cb();
		})
		.catch (function (err){
			cb (err);
		});
	}


	this.cycle = function (){
		timer_ = setInterval(function (){
			me.writeBuffer("flush_cycle");
		}, config.flush_cycle);
	}

	this.close = function (){
		if (timer_)
			clearInterval(timer_);
		buffer_.clear();
		topics_.close();

		me.removeAllListeners();
	}

	// begin cycle flush
	this.cycle();
}
require('util').inherits(Writer, EventEmitter);

module.exports = Writer;