var config = require('../config.json')
	, TimeSeriesBuffer = require('./buffer')
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
	var buffer_ = new TimeSeriesBuffer();
	var timer_ = null;

	// cached topics
	var topics_ = new TopicCache(config.topic_cache);

	// mutexs
	var buffer_write_lock_ = false;

	// status
	// "init", "running", "stopping"
	var status_ = "init"

	this.getStatus = function (){
		return status_;
	}

	//
	// @topic_name - string
	// @data - object
	// @metric - object
	// [@timestamp] - timestamp, number
	// 
	// RETURN
	//		0 : successed
	//		-1 : not running
	//		-2 : beyond max write buffer
	//
	this.append = function (topic_name, data, metric, timestamp){
		if (status_ != "running")
			return -1;
		if (buffer_.size() >= config.max_write_buf)
			return -2;

		buffer_.push(topic_name, data, metric, timestamp);

		if (buffer_.size() >= config.flush_water_level){
			me.writeBuffer("flush_water_level");
		}

		return 0;
	}

	this.writeBuffer = function (reason){
		if (buffer_write_lock_)
			return;

		if (config.debug && config.diagnosis){
			console.log("[%s] Diagnosis:", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("Check buffer, size:%s, reason:%s.", buffer_.size(), reason);
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

		var end_ = function (){
			me.emit('flush_end', reason);				
			buffer_write_lock_ = false;
		}

		// do write parallel
		var write_ = function (){
			if (!buffer_.size()){
				return end_();
			}

			var co_write = 0;
			while(buffer_.size() && (co_write < config.concurrent_write)){
				/*
				// for debug
				if (config.debug)
					console.log("write buffer:%s, co_write:%s, is writting:%s.", 
						buffer_.size(), 
						co_write,
						buffer_write_lock_);
				*/
				co_write++;

				(function (doc){
					setImmediate(function (){
						// diagnosis
						total_write_topic_size ++;
						total_write_buffer_size += doc.series.length;

						write_buf_size ++;
						write_topic_size += doc.series.length;

						// flush to disk
						me.flush(doc, function (err){
							if (err && config.debug)
								console.error(err);

							if (--co_write == 0){
								me.emit('drain');
							}
						});
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

					// finished
					end_();					
				}
			});			
		}

		write_();
	}

	this.flush = function (doc, cb){
		var topic_name = doc.topic;
		var day = doc.day;
		var series = doc.series;
		var metric = doc.metric;

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
		topic.getWriteStream(day, metric, function (err, fstream){
			if (err)
				cb(err);
			else{
				var i = 0, count = series.length;
				var flush_to_disk_ = function (){
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
						fstream.once('drain', flush_to_disk_);
					else{
						cb();
					}

					// Do NOT end the stream
					// Topic will close it automatically.
				};

				flush_to_disk_();
			}
		});
	}


	this.run = function (){
		status_ = "running";
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

	this.stop = function (cb){
		if (status_ == "stopping")
			return;

		// set status
		status_ = "stopping";
		me.once('flush_end', function (reason){
			if (reason == "stop"){
				me.close();
				status_ = "init";				
				cb();
			}
		});

		me.writeBuffer("stop");
	}

	//
	// run cycle
	//
	this.run();
}
require('util').inherits(Writer, EventEmitter);

module.exports = Writer;