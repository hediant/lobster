var Reader = require('../lib/reader');
var moment = require('moment');
var co = require('co');
var Q = require('q');

module.exports = function (topic_name, start, end, cb){
	var ret_ = {};
	var reader_ = new Reader(topic_name);
	var metric_ = null;

	var last_values_ = {};
	var hour_;

	var analogAggr_ = function (type){
		return {
			"type" : type == "Analog" ? "Analog" : "Number",
			"interpolative" : NaN,
			"interpolative_ts" : 0,
			"total" : NaN,
			"totalize_avg" : NaN,
			"count" : 0,
			"avg" : NaN,
			"time_avg" : NaN,
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
				return digitalAggr_();
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
		var hour = moment(ts).format("YYYYMMDD HH");

		if (!hour_ || hour_ < hour){
			hour_ = hour;
			ret_[hour_] = genTheHourAggr_();
		}
		
		var hourAggr = ret_[hour_];

		var calcHourAggr = function (hourAggr){
			for (var field_name in data){
				if (field_name == "__ts__")
					continue;

				var val = data[field_name];
				if (!val)
					continue;

				var aggr = hourAggr[field_name];
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
									aggr.interpolative = 
										last_value.val + 
										(val - last_value.val) * 
										(hts - last_value.ts) / (last_value.ts - ts); 
									aggr.interpolative_ts = hts;
								}
								else{
									aggr.interpolative = val;
									aggr.interpolative_ts = ts;
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
						{
							aggr.series.push(val);
						}
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

		calcHourAggr(hourAggr);
	}

	//
	// Do cursor travel
	//
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
			return cb(null, ret_);

		}).catch(function (err){
			cursor_.close();
			return cb(err);
		});
	});

	cursor_.on('error', function (err){
		cb && cb(err);
	});
}