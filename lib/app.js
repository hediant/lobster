var Metric = require('./metric')
	, MetricCache = require('./metric_cache')
	, Writer = require('./writer')
	, Reader = require('./reader')
	, Q = require('q');

function App(){
	var me = this;
	var metric_cache_ = new MetricCache();
	var writer_ = new Writer();

	//////////////////////////////////////////////////////////
	// metric commands
	this.createMetric = Metric.create;
	this.getMetric = Metric.get;
	this.setMetric = Metric.set;
	this.dropMetric = Metric.drop;
	this.findMetric = Metric.find;

	///////////////////////////////////////////////////////////
	// write commands
	/*
		append data to one topic
	*/
	this.append = function (topic_name, metric_name, data, cb){
		metric_cache_.get(metric_name, function (err, metric){
			if (err)
				cb && cb(err);
			else{
				writer_.append(topic_name, data, metric);
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
				"data" : {object}
			},
			...
		]
	*/
	this.appendBatch = function (batch, cb){
		if (!Array.isArray(batch)){
			return cb && cb("ER_BAD_BATCH_BODY");
		}

		var callbacks = [];
		batch.forEach(function (it){
			callbacks.push(Q.Promise(function (resolve, reject){
				me.append(it.topic, it.metric, it.data, function (err){
					return err ? reject(err) : resolve();
				})
			}))
		})

		Q.allSettled(callbacks, function (results){
			var ret = new Array(results.length);
			results.forEach(function (it, i){
				ret[i] = (it.state == "fulfilled") ? null : it.reason;
			});

			cb && cb(null, ret);
		})
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

		if (query.end == undefined)
			reader.readRaw(parseInt(query.start), options, cb);
		else
			reader.readRaw(parseInt(query.start), parseInt(query.end), options, cb);
	}
}

module.exports = App;