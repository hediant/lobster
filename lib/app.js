var Metric = require('./metric')
	, MetricCache = require('./metric_cache')
	, Writer = require('./writer')
	, Reader = require('./reader')
	, Q = require('q')
	, validate = require('./validate');

function App(env){
	var me = this;
	var metric_cache_ = new MetricCache();
	var writer_ = new Writer();

	//////////////////////////////////////////////////////////
	// metric commands
	this.createMetric = Metric.create;
	this.getMetric = Metric.get;
	this.getMetricByName = Metric.getByName;
	this.setMetric = Metric.set;
	this.setMetricByName = Metric.setByName;
	this.dropMetric = Metric.drop;
	this.dropMetricByName = Metric.dropByName;
	this.findMetric = Metric.find;

	///////////////////////////////////////////////////////////
	// write commands
	/*
		append data to one topic

		@topic_name - string
		@data - object
		@metric_name - string		
		[@timestamp] - timestamp, number
		[@cb] - function (err)
	*/
	this.append = function (){
		var topic_name, data, metric_name, timestamp, cb;
		if (arguments.length < 3)
			throw new Error("At least 3 parameters.");

		topic_name = arguments[0];
		data = arguments[1];		
		metric_name = arguments[2];

		switch(arguments.length){
			case 3:
				break;
			case 4:
				if (typeof arguments[3] == "function")
					cb = arguments[3];
				else
					timestamp = parseInt(arguments[3]);
				break;
			case 5:
			default:
				timestamp = parseInt(arguments[3]);
				cb = arguments[4];
				break;
		}

		if (!validate.topicName(topic_name)){
			return cb && cb("ER_INVALID_TOPIC_NAME");
		}

		metric_cache_.get(metric_name, function (err, metric){
			if (err)
				cb && cb(err);
			else{
				writer_.append(topic_name, data, metric, timestamp);
				cb && cb(null);
			}
		})
	}

	/*
		append batch
		@batch - array
		[
			{
				"topic" : "{topic_name}",
				"metric" : "{metric_name}",
				"data" : {object},
				"ts" : {timestamp}
			},
			...
		]
		@cb - function (err, ret)
		if (err == "ER_PARTIAL_FAIL") 
			ret - array of reason(string)
		else 
			ret - null		
	*/
	this.appendBatch = function (batch, cb){
		if (!Array.isArray(batch)){
			return cb && cb("ER_BAD_BATCH_BODY");
		}

		var append_ = function (topic_name, data, metric_name, timestamp){
			return Q.Promise(function (resolve, reject){
				me.append(topic_name, data, metric_name, timestamp, function (err){
					if (err)
						reject(err);
					else
						resolve();
				})
			})
		}

		var callbacks = [];
		batch.forEach(function (it){
			callbacks.push(append_(it.topic, it.data, it.metric, it.ts));
		});

		Q.allSettled(callbacks).then(function (results){
			var ret = new Array(results.length);
			var last_error = null;

			results.forEach(function (it, i){
				if (it.state == "fulfilled"){
					ret[i] = null;
				}
				else{
					ret[i] = last_error = it.reason;
				}
			});

			if (last_error)
				cb && cb("ER_PARTIAL_FAIL", ret);
			else
				cb && cb(null, null);
		});
	}

	/////////////////////////////////////////////////////////////////////
	// read commands

	/*
		@topic_name - string
		@query - object
		{
			fields : array of string,
			start : timestamp,
			end : timestamp,
			limit : number
		}
	*/
	this.readRaw = function (topic_name, query, cb){
		var reader = new Reader(topic_name, query.fields);
		var options = query.limit ? {"limit":parseInt(query.limit)} : null;
		var start_ = parseInt(query.start);

		if (!validate.topicName(topic_name)){
			return cb && cb("ER_INVALID_TOPIC_NAME");
		}

		if (isNaN(start_)){
			return cb && cb("ER_INVALID_START_TIME");
		}

		if (query.end == undefined){
			reader.readRaw(start_, options, cb);
		}
		else {
			var end_ = parseInt(query.end);
			if (isNaN(end_))
				return cb && cb("ER_INVALID_END_TIME");

			reader.readRaw(start_, end_, options, cb);
		}
	}
}

module.exports = App;