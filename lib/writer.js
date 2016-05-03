var config = require('../config.json');
var Buffer = require('./buffer');
var Q = require('q');
var moment = require('moment');

function Writer(){
	var buffer_ = new Buffer();
	var me = this;
	var timer_ = null;
	var is_buffer_write_ = false;

	this.append = function (topic, data, metric){
		if (buffer_.size() >= config.max_write_buf)
			return -1;

		buffer_.push(topic, data, metric);

		if (buffer_.size() >= config.flush_water_level)
			me.writeBuffer();

		return 0;
	}

	this.writeBuffer = function (){
		if (is_buffer_write_)
			return;

		is_buffer_write_ = true;
		var co_write = 0;

		var write_ = function (){
			while(co_write < config.concurrent_write && buffer_.size()){
				/*
				// for debug
				console.log("write buffer:%s, co_write:%s, is writting:%s.", 
					buffer_.size(), 
					co_write,
					is_buffer_write_);
				*/

				(function (doc){
					co_write ++;
					me.flush(doc, function (err){
						if (err && config.debug)
							console.error(err);

						co_write --;
					});
				})(buffer_.shift());

			}

			if (buffer_.size())
				setImmediate(write_);
			else
				is_buffer_write_ = false;
		}

		write_();
	}

	this.encape = function (data, timestamp, metric){
		var rt = new Array(metric.keys.length + 1);
		rt[0] = moment(timestamp).format("HHmmss");

		metric.keys.forEach(function (key, i){
			rt[i+1] = data.hasOwnProperty(key.name) ? data[key.name] : null;
		});

		return rt.join(config.split) + "\n";
	}

	this.flush = function (doc, cb){
		var metric_count = 0;
		var queue = {};
		var metrics = [];
		var topic = doc.topic;

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
				topic.getWriteStream(metric, function (err, fstream){
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
			me.writeBuffer();
		}, config.flush_cycle);
	}

	this.close = function (){
		if (timer_)
			clearInterval(timer_);
		buffer_.clear();
	}

	// begin cycle flush
	this.cycle();
}

module.exports = Writer;