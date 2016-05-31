var EventEmitter = require('events').EventEmitter
	, co = require('co')
	, Q = require('q');

function Worker(job, options){
	EventEmitter.call(this);
	var me = this;

	// error to retry times
	var retry_ = 0;
	var init_ = false;

	this.addRetryTimes = function (){
		retry_ ++;
	}

	this.getRetryTimes = function (){
		return retry_;
	}

	this.work = function (){
		co(function *(){
			if (!init_ && job.initialize){
				yield job.initialize();
				init_ = true;
			}

			if (job.doWork)
				yield job.doWork();
			
			if (job.onComplited)
				yield job.onComplited();

			return me.emit('completed');
		})
		.catch ((err) => {
			if (job.onError)
				job.onError(err);

			me.emit('error', err);
		});
	}

	this.getJob = function (){
		return job;
	}
}

require('util').inherits(Worker, EventEmitter);
module.exports = Worker;