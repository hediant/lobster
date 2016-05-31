var EventEmitter = require('events').EventEmitter
	, co = require('co')
	, Q = require('q')
	, config = require('../config.json');

function Scheduler(queue){
	var me = this;
	EventEmitter.call(this);

	this.doAJob = function (worker){
		return Q.Promise(function (resolve, reject){
			worker.on('completed', function (){
				resolve("ok");
			});

			worker.on('error', function (err){
				resolve("reject");
			});

			worker.work();
		});
	}

	this.run = function (){
		if (!queue || !queue.size()){
			return me.emit('completed');
		}

		var parallel = 0;
		var worker = queue.next();

		var run_ = function (){
			while(worker && parallel < config.aggregation.parallel_jobs){

				(function (the_worker){
					parallel++;

					co(function *(){
						var state = yield me.doAJob(the_worker);
						if (state != "ok"){
							if (the_worker.getRetryTimes() < config.aggregation.max_retry_times){
								the_worker.addRetryTimes();
								queue.add(the_worker);
								me.emit('job', the_worker.getJob(), "retry");
							}
							else{
								me.emit('job', the_worker.getJob(), "drop")
							}
						}
						else{
							me.emit('job', the_worker.getJob(), "finish");
						}

						if (--parallel == 0){
							if (queue.size()){
								setImmediate(run_);
							}
							else{
								me.emit('completed');
							}
						}

						return;
					});

				})(worker);

				worker = queue.next();
			}			
		}

		run_();
	}
}
require('util').inherits(Scheduler, EventEmitter);

module.exports = Scheduler;