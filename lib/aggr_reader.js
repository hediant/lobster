var config = require('../config.json')
	, MongoClient = require('mongodb').MongoClient
	, co = require('co')
	, Q = require('q')
	, Topic = require('./topic')
	, fs = require('fs')
	, Metric = require('./metric')
	, moment = require('moment')
	, mz = require('moment-timezone');

function AggrReader(topic_name, field_names){
	var me = this;
	var topic_path_ = Topic.BasePath(topic_name);
	var metric_setting_path_ = Topic.MetricSettingPath(topic_name);

	var readMetric_ = function (){
		return Q.Promise((resolve, reject) => {
			fs.readFile(metric_setting_path_, (err, data) => {
				if (err){
					if (err == "ENOENT")
						resolve(null);
					else
						reject(err);
					return;
				}

				var metric_id = data.toString();
				Metric.get(metric_id, (err, results) => {
					if (err){
						if (err == "ER_METRIC_NOT_EXIST")
							resolve(null);
						else
							reject(err);
						return;
					}

					resolve(results);
				})
			})
		})
	}

	var isTopicExists_ = function (){
		return Q.Promise((resolve, reject) => {
			fs.exists(topic_path_, (exists) => {
				resolve(exists);
			});
		});
	}

	var toHourString_ = function (timestamp){
		return mz(timestamp).format("YYYYMMDD HH");
	}

	var toSelection_ = function (metric){
		var sel = {};
		var fields = {};

		metric.keys.forEach(function (it){
			fields[it.name] = it;
		});

		if (field_names && field_names.length){
			field_names.forEach(function (field_name){
				if (fields[field_name]){
					sel[field_name] = fields[field_name];
				}
			})
		}
		else{
			sel = fields;
		}

		return sel;
	}

	var findAndEncape_ = function (h_start, h_end, selection, options){
		return Q.Promise((resolve, reject) => {
			var conditions = {
				"$and" : [
					{"_id" : { "$gte" : h_start }},
					{"_id" : { "$lte" : h_end }}
				]
			}

			co(function *(){
				var db = yield MongoClient.connect(config.mongodb.url);
				var col = db.collection(topic_name);

				var select_mask = {};
				Object.keys(selection).forEach((field_name) => {
					select_mask[field_name] = 1;
				});

				var results = yield col.find(conditions, select_mask).toArray();
				resolve(encapeResults_(results, selection, options));
			})
			.catch((err) => {
				reject(err);
			});
		});
	}

	var fill_with_options_ = function (tag, options){
		var ret = {};

		if (options && options.interval){
			ret["interpolative"] = tag["ip"];
			ret["interpolative_ts"] = tag["ip_ts"];
		}
		if (options && options.total){
			ret["total"] = tag["total"];
		}
		if (options && options.count){
			ret["count"] = tag["count"];
		}
		if (options && options.min){
			ret["min"] = tag["min"];
			ret["min_ts"] = tag["min_ts"];
		}
		if (options && options.max){
			ret["max"] = tag["max"];
			ret["max_ts"] = tag["max_ts"];
		}
		if (options && options.avg){
			ret["avg"] = tag["avg"];
		}
		if (options && options.delta){
			ret["delta"] = tag["delta"];
		}
		if (options && options.stdev){
			ret["stdev"] = tag["stdev"];
		}

		return ret;
	}

	var encapeResults_ = function (results, selection, options){
		var ret = {};
		results.forEach((row) => {
			var ts = mz(row["_id"]).valueOf();
			for (var field_name in row){
				if (field_name == "_id") 
					continue;

				var series = ret[field_name];
				if (series == undefined){
					series = [];
					ret[field_name] = series;
				}

				var tag = row[field_name];
				var data_type = tag.type;

				switch (data_type){
					case undefined:
					case null:
					case "":
					case "Number":
					case "Analog":
						series.push([ts, fill_with_options_(tag, options)]);
						break;
					case "Digital":
						ret[field_name] = series.concat(tag.series);
						break;
					default:
						break;
				}
			}
		});

		return ret;
	}

	var defaultOptions_ = function (){
		return {
			"interval" : "hour",
			"total" : 1,
			"count" : 1,
			"avg" : 1,
			"max" : 1,
			"min" : 1,
			"delta" : 1,
			"stdev" : 0
		}
	}

	/*
		@start - timestamp
		@end - timestamp
		[@options] - object
		{
			"interval" : "hour" || "day",
			"total" : 1 || 0,
			"count" : 1 || 0,
			"avg" : 1 || 0,
			"min" : 1 || 0,
			"max" : 1 || 0,
			"delta" : 1 || 0,
			"stdev" : 1 || 0
		}
	*/
	this.find = function (start, end, options, cb){
		if (!cb){
			return Q.Promise((resolve, reject) => {
				me.find(start, end, options, (err, results) => {
					err ? reject(err) : resolve(results);
				})
			})
		}

		var h_start = toHourString_(start);
		var h_end = toHourString_(end || Date.now());

		// fill options
		var options_ = options ? options : defaultOptions_();

		co(function *(){
			if (!(yield isTopicExists_()))
				return cb("ER_TOPIC_NOT_EXIST");
			
			var metric = yield readMetric_();
			if (!metric)
				return cb("ER_NO_DATA");

			var selection = toSelection_(metric);
			var results = yield findAndEncape_(h_start, h_end, selection, options_);

			cb (null, results);
		})
		.catch(cb);
	}
}

module.exports = AggrReader;