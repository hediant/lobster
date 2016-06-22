var Scheduler = require('../scheduler')
	, Queue = require('../queue')
	, Worker = require('../worker')
	, Job = require('../jobs/aggregation')
	, AggrIndexJob = require('../jobs/create_aggr_index')
	, moment = require('moment')
	, config = require('../../config.json')
	, CronJob = require('cron').CronJob
	, Q = require('q')
	, co = require('co');

var next = null;
var is_running = false;

var doCreateAggrIndexJobs = function (){
	return Q.Promise((resolve, reject) => {
		var queue = new Queue();
		for (var i=0; i<config.regions; i++){
			var region_idx = i;
			var job = new AggrIndexJob(region_idx);

			queue.add(new Worker(job));
		}

		var start = Date.now();
		var sdlr = new Scheduler(queue);

		sdlr.on('completed', function (){
			var end = Date.now();
			console.log("===============================================");
			console.log("Create Indexes completed! cost:%s.", moment.duration((end - start)).humanize());
			console.log("** %s **", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("");

			resolve();	
		});

		sdlr.on('job', function (job, state){
			console.log("[%s] Create index of region:%s, state:%s.", 
				moment().format("YYYY-MM-DD HH:mm:ss"), 
				job.getName(),
				state);
		});

		console.log("===============================================");
		console.log("Begin create indexes");
		console.log("");

		sdlr.run();		
	})
	.catch ((err) => {
		reject(err);
	});
}

var doAggregateJobs = function (){
	return Q.Promise((resolve, reject) => {
		var jobs = Job.fetch();
		var queue = new Queue();
		jobs.forEach(function (job){
			queue.add(new Worker(job));
		});

		var progress = 0;
		var start = Date.now();

		var sdlr = new Scheduler(queue);
		sdlr.on('completed', function (){
			var end = Date.now();
			console.log("===============================================");
			console.log("All task completed! cost:%s.", moment.duration((end - start)).humanize());
			console.log("** %s **", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("");

			resolve();
		});

		sdlr.on('job', function (job, state){
			console.log("[%s] Aggregate %s, state:%s.", 
				moment().format("YYYY-MM-DD HH:mm:ss"), 
				job.getName(), 
				state);

			if (state == "finish"){
				progress ++;
			}

			if (progress % 100 == 0){
				console.log("===============================================");
				console.log("[%s] Total progress: %s\%", 
					moment().format("YYYY-MM-DD HH:mm:ss"),
					(progress/jobs.length * 100).toFixed(1));
				console.log("===============================================");
			}
		});

		sdlr.run();	
	})
	.catch ((err) => {
		reject(err);
	});
}

var cronJob = new CronJob({
	// Runs every day at 00:10:00 AM 
	"cronTime" : "* 00 10 00 * * *",

	//	To do aggregation job
	"onTick" : function (){
		var doAggregateJob_ = function (){
			if (is_running){
				console.log("===============================================");
				console.log("** %s **", moment().format("YYYY-MM-DD HH:mm:ss"));
				console.log("* Last task has not completed, set next job.");
				console.log("===============================================");

				next = doAggregateJob_;
				return;
			}

			next = null;
						
			console.log("============================================");
			console.log("** Begin Aggregation Jobs ");
			console.log("** %s", moment().format("YYYY-MM-DD HH:mm:ss"));
			console.log("============================================");

			co(function *(){
				/*
					Do Aggregation Jobs
				*/
				yield doAggregateJobs();

				/*
					Create Aggregation Indexes
				*/
				yield doCreateAggrIndexJobs();

				/*
					Check next state
				*/
				if (next){
					setImmediate(next);
				}

				// set running status				
				is_running = false;

			})
			.catch ((err) => {
				/*
					Error to exit process, pm2 will restart it.
				*/
				console.error("!!==================================");
				console.error("Fatal Error:%s, we must exit the process.", err);
				console.error("!!==================================");
				
				process.exit(1);				
			})
		}

		doAggregateJob_();
	},

	// Start the job right now
	"runOnInit" : true,

	// Time zone of this job.
	// "timeZone" : "Asia/Beijing"
});

cronJob.start();

