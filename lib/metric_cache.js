var Metric = require('./metric');

function MetricCache(){
	var me = this;
	var metrics_ = {};

	this.get = function (metric_name, cb){
		var the_metric = metrics_[metric_name];
		if (the_metric != undefined){
			return cb && cb(null, the_metric);
		}

		// load metric
		Metric.find({"name":metric_name}, {"limit":1}, function (err, results){
			if (err)
				cb && cb(err);
			else {
				if (!results.length)
					cb && cb("ER_METRIC_NOT_EXIST");
				else{
					the_metric = results[0];
					metrics_[metric_name] = the_metric;

					cb && cb(null, the_metric);
				}
			}
		})
	}
}

module.exports = MetricCache;