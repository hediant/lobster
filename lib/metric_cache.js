var Metric = require('./metric')
	, Subpub = require('./subpub')
	, Subscribe = Subpub.Subscribe;

function MetricCache(){
	var me = this;
	var metrics_ = {};
	var metric_idx_ = {};

	var lock_ = false;
	var subpub_ = new Subscribe();

	var onChange_ = function (topic, id){
		if (topic == "metric"){
			var metric_name = metric_idx_[id];
			if (metric_name){
				delete metrics_[metric_name];
				delete metric_idx_[id];
			}
		}
	}

	this.get = function (metric_name, cb){
		if (lock_){
			setImmediate(function (){
				me.get(metric_name, cb);
			});

			return;
		}

		lock_ = true;

		var the_metric = metrics_[metric_name];
		if (the_metric != undefined){
			cb && cb(null, the_metric);
			lock_ = false;
			return;
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
					metric_idx_[the_metric.id] = metric_name;

					cb && cb(null, the_metric);
				}
			}

			lock_ = false;
		})
	}

	this.init = function (){
		// on 'create', do nothing
		subpub_.on('modify', onChange_);
		subpub_.on('remove', onChange_);

		subpub_.sub();
	}

	this.close = function (){
		subpub_.close();
	}

	this.init();
}

module.exports = MetricCache;