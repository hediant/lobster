var EventEmitter = require('events').EventEmitter
	, path = require('path')
	, config = require('../config.json')
	, moment = require('moment')
	, fs = require('fs')
	, Q = require('q')
	, Metric = require('./metric')
	, csv = require('csv-parser')
	, sortBy = require('sort-array')
	, insertionSort = require('../utils/insertion_sort')
	, Topic = require('./topic')
	, type_cast = require('./type_cast')
	, isEmptyOrNull = require('../utils/is_empty_or_null')
	, co = require('co')
	, LBF = require('./lbf');

/*
	@topic_name - string, topic name
	[@field_names] - array of string, select which field names (keys) of metric
*/
function Reader(topic_name, field_names){
	EventEmitter.call(this);

	var me = this;
	var base_path_ = Topic.BasePath(topic_name);
	var metric_setting_ = Topic.MetricSettingPath(topic_name);
	var one_day_ = moment.duration(1, "days");
	var max_ret_count_ = 100000;

	this.getTopicName = function (){
		return topic_name;
	}

	this.getFieldNames = function (){
		return field_names;
	}

	var getDayFromHandle = function (handle){
		return path.basename(handle).substr(0, 8);
	}

	// @cb - function (err, metric)
	var getMetric_ = function (cb){
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
	// @metric - object
	// @cb - function (err, handles)
	var getHandles_ = function (metric, cb){
		var handles = {};

		var metric_path = Topic.MetricPath(topic_name, metric.id);
		fs.readdir(metric_path, function (err, files){
			if (err){
				if (err.code == "ENOENT")
					cb("ER_TOPIC_NOT_EXIST");
				else
					cb("ER_IO_ERROR");
				return;				
			}

			files.sort().forEach(function (file){
				if (path.extname(file) != Topic.LogFileExtName())
					return;

				var day = getDayFromHandle(file);
				if (!handles[day]){
					handles[day] = [];
				}

				handles[day].push(path.join(metric_path, file));
			});

			cb(null, handles);
		});
	}

	// @start - timestamp, number
	// @end - timestamp, number
	// assert start <= end
	var getDayRange_ = function (start, end, handles){
		var days = Object.keys(handles);
		var s_day = moment(start).format("YYYYMMDD");
		var e_day = moment(end).format("YYYYMMDD");

		var rt_days = [];

		days.forEach(function (day){
			if (day >= s_day && day <= e_day){
				rt_days.push(day);
			}
		})

		return rt_days.sort();
	}


	var readRawSeries_ = function (start, end, options, cb){
		var cursor = new Cursor(start, options);
		var count = 0;
		var recursive_deep = 0;

		/*
			recursive performance is 4-5x then Generator/Promise.
		*/
		cursor.once('ready', function (){
			var ret = {};
			var select = cursor.getSelections();
			if (select && select.length){
				select.forEach(function (field){
					ret[field.name] = [];
				})
			}

			var fill_and_return_ = function (data){
				for (var key in data){
					if (key == "__ts__")
						continue;

					var the_field = ret[key];
					if (the_field){
						the_field.push([data.__ts__, data[key]]);
						
						if (++count >= options.limit)
							return true;
					}
				}

				return false;
			}

			var iter_ = function (err, data){
				if (err){
					cursor.close();
					return cb && cb(err);
				}

				if (data){
					if (fill_and_return_(data)){
						cursor.close();
						cb && cb(null, ret);
					}
					else{
						if (++recursive_deep > 1000){
							setImmediate(function (){
								recursive_deep = 0;
								cursor.next(iter_);
							});	
						}
						else{
							cursor.next(iter_);
						}				
					}
				}
				else{
					// no more data
					cursor.close();
					cb && cb(null, ret);
				}
			}

			cursor.next(iter_);
		});

		cursor.once('error', function (err){
			cb && cb(err);
		});
	}

	var readRawSeriesES6_ = function (start, end, options, cb){
		var cursor = new Cursor(start, options);
		var count = 0;
		cursor.on('ready', function (){
			var ret = {};
			var select = cursor.getSelections();
			if (select && select.length){
				select.forEach(function (field){
					ret[field.name] = [];
				})
			}

			co(function *(){
				var data = yield cursor.next();
				while(data && count < options.limit){
					count ++;
					var ts = data["__ts__"];
					for (var key in data){
						if (ret[key]){
							ret[key].push([ts, data[key]]);
						}
					}

					data = yield cursor.next();
				}

				cursor.close();
				cb(null, ret);
			})
			.catch ((err) => {
				cursor.close();
				cb (err);
			});
		});

		cursor.on('error', (err) => {
			cursor.close();
			cb (err);
		})		
	}

	/*
		@start - start timestamp
		[@end] - end timestamp
		[@options] - object, options
		{
			limit : max to 10000
			where : function (row)

		}
		@cb - function (err, data) 
	*/
	this.readRaw = function (){
		if (arguments.length < 2)
			throw new Error("At least 2 parameters.");
		switch(arguments.length){
			case 2:
				return me.readRaw(arguments[0], undefined, {}, arguments[2])
			case 3:
				if (typeof arguments[1] == "number"){
					// end
					return me.readRaw(arguments[0], arguments[1], {}, arguments[2]);
				}
				else{
					// options
					return me.readRaw(arguments[0], undefined, arguments[1], arguments[2]);
				}
			case 4:
			default:
				var start = arguments[0];
				var end = arguments[1];
				var options = arguments[2];
				var cb = arguments[3];

		}

		var start_ = start, end_ = (end == undefined) ? Date.now() : end;
		var options_ = {
			"limit" : max_ret_count_,
			"where" : function (row){
				return true;
			}
		};

		if (options && options.limit){
			options_.limit = parseInt(options.limit) < max_ret_count_ ? parseInt(options.limit) : max_ret_count_;
		}
		if (options && typeof options.where == "function"){
			options_.where = options.where;
		}

		// The maximum time range of a query is 1 weeks
		var a_week = moment.duration(1, "weeks").valueOf();
		if (Math.abs(end_ - start_) > a_week){
			cb && cb("ER_BAD_DATE_RANGE");
			return;
		}

		// add end options
		options_.end = end_;

		// do query series
		// readRawSeriesES6_(start_, end_, options_, cb);
		readRawSeries_(start_, end_, options_, cb);
	}

	this.cursor = function (start, options){
		return new Cursor(start, options);
	}

	// 
	// @start - timestamp
	/* [@options] - object
		{
			forward : true(default) | false,
			where : function (row),
			end : timestamp
		}
	*/
	// 
	var Cursor = function (start, options){
		var the_cursor = this;
		EventEmitter.call(this);

		// status
		var cursor_ready_ = false;
		// metric of the topic
		var metric_ = null; 
		// all handles of a topic
		var handles_ = null;
		// select fields to query, array of object
		var select_fields_ ;

		// days range
		var days_;
		// data of current block
		var buffer_;
		// current day
		var current_;
		// offset to current block
		var offset_ = 0;

		// default options 
		var options_ = {
			"forward" : true,
			"where" : function (row){
				return true;
			}
		};

		var setFieldsDef_ = function (){
			select_fields_ = [];
			if (!field_names || !Array.isArray(field_names)){
				select_fields_ = metric_.keys;
			}
			else{
				metric_.keys.forEach(function (field){
					if (field_names.indexOf(field.name) != -1){
						select_fields_.push(field);
					}
				});
			}

			return select_fields_;
		}

		this.getSelections = function (){
			return select_fields_;
		}

		var setOptions_ = function (){
			if (options && options.forward != undefined){
				options_.forward = options.forward ? true : false
			}

			if (options && options.end != undefined){
				options_.end = options.end;
			}
			else{
				options_.end = options_.forward ? Date.now() : 0;
			}

			if (options && typeof options.where == "function"){
				options_.where = options.where;
			}

			// set forward
			if (options_.end < start)
				options_.forward = false;
		}

		this.getMetric = function (){
			return metric_;
		}

		this.init = function (){
			Q.fcall(function (){
				// if topic exist?
				return Q.Promise(function (resolve, reject){
					fs.exists(base_path_, function (exist){
						if (!exist)
							reject("ER_TOPIC_NOT_EXIST");
						else
							resolve();
					})
				})
			}).then(function (){
				// get metric and set fields definition
				return Q.Promise(function (resolve, reject){
					getMetric_(function (err, results){
						if (err){
							if (err == "ER_METRIC_NOT_EXIST")
								reject("ER_NO_DATA");
							else
								reject(err);
						}
						else{
							metric_ = results;
							setFieldsDef_();
							resolve();
						}
					})
				})
			}).then(function (){
				// get handles
				return Q.Promise(function (resolve, reject){
					getHandles_(metric_, function (err, results){
						if (err)
							reject(err);
						else{
							handles_ = results;
							if (options_.forward)
								days_ = getDayRange_(start, options_.end, handles_);
							else
								days_ = getDayRange_(options_.end, start, handles_);

							cursor_ready_ = true;
							the_cursor.emit('ready');			
							resolve();
						}
					})
				})
			}).catch (function (err){
				the_cursor.emit('error', err);
			});			
		}

		this.close = function (){
			the_cursor.removeAllListeners();
			cursor_ready_ = false;
		}

		var inRange_ = function (ts){
			if (options_.forward)
				return ts >= start && ts < options_.end;
			else
				return ts > options_.end && ts <= start;
		}

		var sortBuffer_ = function (results){
			/*
				Sorting cost accounted for more than 60% of the query time.
				TODO: C++ Module for optimization
			*/
			//return sortBy(results, "__ts__");
			return insertionSort(results, function (x, y){return x.__ts__ > y.__ts__; });
		}

		var readHandles_ = function (handles, cb){
			var callbacks = [];
			handles.forEach(function (handle){
				callbacks.push(readOneHandle_(handle));
			});

			var last_error = null;
			Q.allSettled(callbacks).then(function (results){
				var rows = [];
				results.forEach(function (it, idx){
					if (it.state == "rejected"){
						last_error = "ER_READ_HANDLE";

						if (config.debug){
							console.error("Read [%s] error, %s.", handles[idx], it.reason);
						}

						return;
					}

					if (Array.isArray(it.value)){
						rows = rows.concat(it.value);
					}
				})

				cb && cb(last_error ? "ER_PART_RESULTS" : null, rows);
			});			
		}

		var readOneHandle_ = function (handle){
			return Q.Promise(function (resolve, reject){
				var rows = [];
				var t1 = Date.now();
				fs.readFile(handle, function (err, data){
					var t2 = Date.now();
					if (err)
						return reject(err);

					if (data.length < LBF.LBF_FLAG_LEN){
						return reject("ER_INVALID_LOG_FILE");
					}

					// wrapper to ByteBuffer
					var buffer = LBF.wrap(data);

					// check flag if it's a lbf file
					var flag = LBF.readFlag(buffer);
					if (!LBF.isMatchedFlag(flag))
						return reject("ER_INVALID_LOG_FILE");

					// parse data
					rows = transform_(buffer, handle);
					var t3 = Date.now();

					if (config.debug){
						console.log("Load file %s: %s ms, parse: %s ms, total: %s ms.",
							path.relative(config.db_path, handle),
							(t2 - t1),
							(t3 - t2),
							(t3 - t1));
					}

					resolve(rows);
				})
			})
		}

		var chunkToObject_ = function (chunk){
			var data = {};
			metric_.keys.forEach(function (field, i){
				var val = chunk[i];
				if (isEmptyOrNull(val))
					return;
				data[field.name] = val;
			});	
			
			return data;	
		}

		var transform_ = function (buffer, handle){
			var rows = [];
			var evt = LBF.Event.nextOne(buffer);

			while(evt){
				var ts = evt.header.timestamp;
				if (!inRange_(ts) || !evt.chunk){
					// next event
					evt = LBF.Event.nextOne(buffer);					
					continue;
				}

				try {
					// parse chunk (event body) to array
					var chunk = LBF.Event.parseBody(evt.chunk);
	
					// array to object
					var data = chunkToObject_(chunk);

					// filter with selection
					var ret = {};
					if (options_.where(data)){
						select_fields_.forEach(function (field){
							var val = data[field.name];
							if (!isEmptyOrNull(val)){
								ret[field.name] = val;
							}
						});	
						ret["__ts__"] = ts;	
						rows.push(ret);			
					}		
				}
				catch (err){
					// chunk is broken!
					if (config.debug){
						console.error("File: %s chunk broken, err:%s.", handle, err);
						console.error(err.stack);
					}
				}

				// next event
				evt = LBF.Event.nextOne(buffer);

			}

			return rows;
		}		

		// 
		// @cb - function (err, data)
		//		data == null, means the end of cursor
		//
		this.next = function (cb){
			if (typeof cb == "function"){
				if (!cursor_ready_)
					throw new Error("Cursor must be init and ready before next().");

				options_.forward ? forward_(cb) : backward_(cb);
			}
			else{
				return nextPromise_();
			}
		}

		var nextPromise_ = function (){
			return Q.Promise(function (resolve, reject){
				var next = options_.forward ? forward_ : backward_;
				next(function (err, data){
					if (err)
						reject(err);
					else
						resolve(data);
				});
			});
		}

		var forward_ = function (cb){
			if (!select_fields_ || !select_fields_.length)
				return cb && cb(null, null);

			if (!buffer_){
				current_ = days_.shift();
				if (!current_){
					return cb && cb(null, null);
				}

				// load buffer_
				readHandles_(handles_[current_], function (err, results){
					if (err && err != "ER_PART_RESULTS"){
						cb && cb(err);
					}
					else{
						buffer_ = sortBuffer_(results);
						offset_ = 0;

						forward_(cb);
					}
				});	
			}
			else {
				var data = buffer_[offset_++];
				if (data){
					cb && cb(null, data);
				}
				else{
					buffer_ = null;
					forward_(cb);
				}
			}
		}

		var backward_ = function (cb){
			if (!select_fields_ || !select_fields_.length)
				return cb && cb(null, null);

			if (!buffer_){
				current_ = days_.pop();
				if (!current_){
					return cb && cb(null, null);
				}

				// load buffer_
				readHandles_(handles_[current_], function (err, results){
					if (err && err != "ER_PART_RESULTS"){
						cb && cb(err);
					}
					else{
						buffer_ = sortBuffer_(results);
						offset_ = buffer_.length;

						backward_(cb);
					}
				});	
			}
			else {
				var data = buffer_[--offset_];
				if (data){
					cb && cb(null, data);
				}
				else{
					buffer_ = null;
					backward_(cb);
				}
			}
		}			

		// set options
		setOptions_();	
			
		// do init
		this.init();
	}
	require('util').inherits(Cursor, EventEmitter);
}
require('util').inherits(Reader, EventEmitter);

module.exports = Reader;

