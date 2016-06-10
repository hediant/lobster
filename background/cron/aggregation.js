var Scheduler = require('../scheduler')
	, Queue = require('../queue')
	, Worker = require('../worker')
	, Job = require('../jobs/aggregation')
	, moment = require('moment')
	, config = require('../../config.json')
	, CronJob = require('cron').CronJob;

var next = null;
var is_running = false;

var doAggregateJob = function (){
	if (is_running){
		next = doAggregateJob;
		return;
	}
	
	next = null;

	var jobs = Job.fetch();
	var queue = new Queue();
	jobs.forEach(function (job){
		queue.add(new Worker(job));
	});

	var progress = 0;
	var start = Date.now();

	var sdlr = new Scheduler(queue);
	sdlr.on('completed', function (){
		is_running = false;
		if (next){
			setImmediate(next);
		}

		var end = Date.now();
		console.log("===============================================");
		console.log("All task completed! cost:%s.", moment.duration((end - start)).humanize());
		console.log("** %s **", moment().format("YYYY-MM-DD HH:mm:ss"));
		console.log("");
	});

	sdlr.on('job', function (job, state){
		if (config.debug){
			console.log("[%s] Aggregate %s, state:%s.", 
				moment().format("YYYY-MM-DD HH:mm:ss"), 
				job.getName(), 
				state);
		}

		if (state == "finish"){
			progress ++;
		}

		if (progress % Math.floor(jobs.length / 100) == 0){
			console.log("===============================================");
			console.log("[%s] Total progress: %s\%", 
				moment().format("YYYY-MM-DD HH:mm:ss"),
				Math.floor(progress/jobs.length * 100));
			console.log("===============================================");
		}
	});

	sdlr.run();	
}

var cronJob = new CronJob({
	// Runs every day at 00:10:00 AM 
	"cronTime" : "* 00 10 00 * * *",

	//	To do aggregation job
	"onTick" : function (){
		console.log("============================================");
		console.log("** Begin Aggregation Jobs ");
		console.log("** %s", moment().format("YYYY-MM-DD HH:mm:ss"));
		console.log("============================================");

		doAggregateJob();
	},

	// Start the job right now
	"runOnInit" : true,

	// Time zone of this job.
	// "timeZone" : "Asia/Beijing"
});

cronJob.start();

