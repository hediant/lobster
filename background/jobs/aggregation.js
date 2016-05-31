var config = require('../../config')
	, path = require('path')
	, fs = require('fs')
	, moment = require('moment')
	, Q = require('q')
	, Aggregation = require('../../lib/aggregation');

function AggrJob(topic_name){
	var me = this;
	var aggr_path_ = path.join(config.db_path, topic_name, "aggregation");

	var start_ = 0;
	// beginning of current day
	var end_ = moment(0, "HH").valueOf();

	var no_need_complite_ = false;

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
			Aggregation(topic_name, start_, end_, (err) => {
				if (err){
					if (err == "ER_TOPIC_NOT_EXIST"){
						no_need_complite_ = true;
						resolve();
					}
					else if (err == "ER_NO_DATA"){
						resolve();
					}
					else
						reject(err)
				}
				else
					resolve();
			});			
		})
	}

	this.onComplited = function (){
		return Q.Promise((resolve, reject) => {
			if (no_need_complite_)
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
	var topics = fs.readdirSync(config.db_path);
	var tasks = new Array(topics.length);

	topics.forEach(function (topic_name, idx){
		tasks[idx] = new AggrJob(topic_name);
	});

	return tasks;
}

module.exports = AggrJob;