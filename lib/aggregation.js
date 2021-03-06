var Reader = require('./reader')
	, moment = require('moment')
	, co = require('co')
	, Q = require('q')
	, obj_values = require('../utils/obj_values');

/*
	aggregation example

	return Array of document

	Analog Data Type - document[i]
	{
		"topic" : topic_name,
		"ts" : <timestamp>,
		"fields" : {
			"a_1" : {
				"type" : "Analog",
				"ip" : 388676.13,
				"ip_ts" : 1463994144000,
				"total" : 5462054.1899999995,
				"count" : 11,
				"avg" : 496550.38090909086,
				"min" : 17076.31,
				"min_ts" : 1463996064000,
				"max" : 823376.19,
				"max_ts" : 1463996174000,
				"delta" : NaN,
				"stdev" : NaN
			},
			"a_2" : {
				...
			},
			... ...	
		}
	}

*/
var aggregate = function (topic_name, start, end, cb){
	if (!cb){
		return Q.Promise((resolve, reject) => {
			aggregate(topic_name, start, end, (err, results) => {
				err ? reject(err) : resolve(results);
			});
		});
	}

	var results_ = {};

	var reader_ = new Reader(topic_name);
	var metric_ = null;

	var last_values_ = {};
	var hour_;

	var analogAggr_ = function (type){
		return {
			"type" : type == "Analog" ? "Analog" : "Number",

			// interpolative
			"ip" : NaN,
			"ip_ts" : 0,

			"total" : NaN,
			//"totalize_avg" : NaN,
			"count" : 0,
			"avg" : NaN,
			//"time_avg" : NaN,
			"min" : NaN,
			"min_ts" : 0,
			"max" : NaN,
			"max_ts" : 0,
			"delta" : NaN,
			"stdev" : NaN
		}
	}

	var digitalAggr_ = function (){
		return {
			"type" : "Digital",
			"series" : []
		}
	}

	var createHourAggr_ = function (data_type){
		switch(data_type){
			case undefined:
			case null:
			case "":
			case "Analog":
			case "Number":
				return analogAggr_(data_type);
			case "Digital":
				//return digitalAggr_();
			default:
				return null;
		}
	}

	var genTheHourAggr_ = function (){
		var hour_aggr = {};
		metric_.keys.forEach(function (key){
			hour_aggr[key.name] = createHourAggr_(key.type);
		});

		return hour_aggr;
	}

	var calcAggregation_ = function (data){
		var ts = data["__ts__"];
		var hour = moment(moment(ts).toArray().slice(0,4)); 	// only hours
		var doc;
		
		if (!hour_ || hour_ < hour){
			hour_ = hour;
			doc = {
				"topic" : topic_name,
				"ts" : hour.valueOf(),
				"fields" : genTheHourAggr_()
			};

			results_[hour] = doc;
		}
		else {
			doc = results_[hour];
		}

		var calcHourAggr = function (aggr_fields){
			for (var field_name in data){
				if (field_name == "__ts__")
					continue;

				var val = data[field_name];
				if (!val)
					continue;

				var aggr = aggr_fields[field_name];
				if (!aggr)
					continue;

				var last_value = last_values_[field_name];

				switch (aggr.type){
					case undefined:
					case null:
					case "":
					case "Analog":
					case "Number":
						{			
							// count				
							aggr.count ++;

							// interpolative && interpolative timestamp
							if (aggr.count == 1){
								var hts = moment(hour_).valueOf();
								if (last_value && last_value.val){
									aggr.ip = 
										last_value.val + 
										(val - last_value.val) * 
										(hts - last_value.ts) / (last_value.ts - ts); 
									aggr.ip_ts = hts;
								}
								else{
									aggr.ip = val;
									aggr.ip_ts = ts;
								}
							}

							// delta
							if (last_value && last_value.val){
								if (aggr.count == 1)
									aggr.delta = 0;
								else
									aggr.delta += (val - last_value.val);							
							}

							// total
							if (!aggr.total)
								aggr.total = 0;
							aggr.total += val;

							// average
							aggr.avg = aggr.total / aggr.count;
							
							// minimum && minimun timestamp
							if (!aggr.min || val < aggr.min){
								aggr.min = val;
								aggr.min_ts = ts;
							}

							// maximum && maximum timestamp
							if (!aggr.max || aggr.max < val){
								aggr.max = val;
								aggr.max_ts = ts;
							}
						}
						break;
					case "Digital":
						//aggr.series.push([ts, val]);
						break;
					default:
						break;				
				}

				last_values_[field_name] = {
					"val" : val,
					"ts" : ts
				}
			}
		}

		calcHourAggr(doc.fields);
	}

	//
	// Do cursor travel
	//

	var cursorWithES6_ = function (){
		var cursor_ = reader_.cursor(start, {"end":end});
		cursor_.on('ready', function (){
			// Set metric
			metric_ = cursor_.getMetric();
			// Generate Return Structure
			createHourAggr_();

			co(function *(){
				var data = yield cursor_.next();
				while(data){
					calcAggregation_(data);
					data = yield cursor_.next();
				}

				cursor_.close();
				return cb(null, obj_values(results_));

			}).catch(function (err){
				cursor_.close();
				return cb(err);
			});
		});

		cursor_.on('error', function (err){
			cb && cb(err);
		});
	}

	var cursorWithRecursive_ = function (){
		var cursor_ = reader_.cursor(start, {"end":end});
		var recursive_deep = 0;

		/*
			recursive performance is 4-5x then Generator/Promise.
		*/
		cursor_.once('ready', function (){
			// Set metric
			metric_ = cursor_.getMetric();
			// Generate Return Structure
			createHourAggr_();

			var iter_ = function (err, data){
				if (err){
					cursor_.close();
					return cb && cb(err);
				}

				if (data){
					calcAggregation_(data);

					if (++recursive_deep > 1000){
						setImmediate(function (){
							recursive_deep = 0;
							cursor_.next(iter_);
						});	
					}
					else{
						cursor_.next(iter_);
					}				
				}
				else{
					// no more data
					cursor_.close();
					cb && cb(null, obj_values(results_));
				}
			}

			cursor_.next(iter_);
		});

		cursor_.once('error', function (err){
			cb && cb(err);
		});
	}

	//
	// Do All Aggregation jobs
	//

	// cursorWithES6_();
	cursorWithRecursive_();

}

module.exports = aggregate;