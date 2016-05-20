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

		var showProgress_ = function (){
			console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("Buffer flushed:%s (topics:%s), dirty:%s.",
				write_buf_size,
				write_topic_size,
				buffer_.size());

			write_buf_size = 0;
			write_topic_size = 0;
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

				(function (doc){
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

						if (co_write == 0){
							me.emit('drain');
						}
					});
				})(buffer_.shift());

			}


			me.once('drain', function (){
				if (buffer_.size()){
					showProgress_();

					// write again
					write_();
				}
				else{
					end_write_buf = Date.now();

					if (config.diagnosis){
						console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
						console.log("Flush completed, size:%s, topics:%s, cost:%sms.",
							total_write_buffer_size,
							total_write_topic_size,
							(end_write_buf - start_write_buf));
					}

					// clear mutex
					buffer_write_lock_ = false;						
				}
			});
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
		var topic_name = doc.topic;
		var day = doc.day;

		// get topic
		var topic = topics_.get(topic_name);
		if (!topic){
			topic = new Topic(topic_name);
			topic.once('ready', function (){
				topics_.set(topic_name, topic);
				me.flush(doc, cb);
			});
			topic.once('error', function (err){
				cb(err);
			});

			return;
		}

		//
		// main procedure to flush a topic's data
		//
		var metric_count = 0;
		var queue = {};
		var metrics = [];

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
				topic.getWriteStream(day, metric, function (err, fstream){
					if (err)
						reject(err);
					else{
						var i = 0, count = series.length;
						var write_ = function (){
							var ok = true;
							while(i < count && ok) {
								/*
									// for debug
									console.log("metric:",metric.id)
									console.log(fstream.path)
									console.log(series[i])
								*/
								ok = fstream.write(series[i++]);
							};

							if (i < count)
								fstream.once('drain', write_);
							else
								resolve();

							// Do NOT end the stream
							// Topic will close it automatically.
						};

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
			cb(null);
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