var config = require('../../config')
	, path = require('path')
	, fs = require('fs')
	, moment = require('moment')
	, Q = require('q')
	, Aggregation = require('../../lib/aggregation')
	, AggrWriter = require('../../lib/aggr_writer')
	, Topic = require('../../lib/topic')
	, co = require('co');

function AggrJob(topic_name){
	var me = this;
	var aggr_path_ = path.join(Topic.BasePath(topic_name), "aggregation");

	var start_ = 0;
	// beginning of current day
	var end_ = moment(0, "HH").valueOf();

	var do_not_save_last_flag_ = false;

	this.getName = function (){
		return topic_name;
	}

	this.initialize = function (){
		return Q.Promise((resolve, reject) => {
			fs.readFile(aggr_path_, function (err, data){
				if (err){
					if (err.code == "ENOENT")
						resolve();
					else
						reject(err);
				}
				else{
					start_ = moment(data.toString()).valueOf();
					resolve();
				}
			});				
		})
	}

	this.doWork = function (){
		return Q.Promise((resolve, reject) => {
			co(function *(){
				var t1 = t2 = t3 = 0;
				var showProgress = function (){
					if (config.diagnosis){
						console.log("Aggregate %s, [%s,%s), calc:%s ms, write:%s ms, total:%s ms.", 
							topic_name,
							moment(start_).format('YYMMDD'),
							moment(end_).format('YYMMDD'),
							(t2 - t1), 
							(t3 - t2),
							(t3 - t1));
					}
				}

				if (start_ == end_){
					showProgress();
					return resolve();
				}

				t1 = Date.now();
				var aggr = yield Aggregation(topic_name, start_, end_);

				t2 = Date.now();
				var writer = new AggrWriter(topic_name);
				yield writer.save(aggr);

				t3 = Date.now();
				showProgress();

				resolve();
			})
			.catch ((err) => {
				if (err == "ER_TOPIC_NOT_EXIST"){
					do_not_save_last_flag_ = true;
					resolve();
				}
				else if (err == "ER_NO_DATA"){
					resolve();
				}
				else
					reject(err)	;			
			});
		});
	}

	this.onComplited = function (){
		return Q.Promise((resolve, reject) => {
			if (do_not_save_last_flag_)
				return resolve();

			var last = moment(end_).format('YYYYMMDD');
			fs.writeFile(aggr_path_, last, (err) => {
				if (err)
					reject(err);
				else
					resolve();
			});			
		})
	}
}

//
// @cb - function (err, tasks)
//
AggrJob.fetch = function (){
	var regions = fs.readdirSync(config.db_path);
	var tasks = [];

	regions.forEach(function (region_idx){
		var region_path = path.join(config.db_path, region_idx);
		var topics = fs.readdirSync(region_path);

		topics.forEach(function (topic_name){
			tasks.push(new AggrJob(topic_name));
		});
	});

	return tasks;
}

module.exports = AggrJob;