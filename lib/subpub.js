var EventEmitter = require('events').EventEmitter
	, mysql = require('mysql')
	, obj_values = require('../utils/obj_values')
	, global = require("../global")
	, SimpleOrm = require("../../simpleorm")
	, Dbhelper = SimpleOrm.DbHelper;

//
// 获得最后修改的版本
// 如果没有任何更新的主题时间，则返回"NO_UPDATE_TOPICS"
//
var lastVersion = function (cb) {
	var dbhelper = new Dbhelper(global.DbConnection);
	dbhelper.getConnection(function(err, connection){
		connection.query("select ver from t_updates order by ver desc limit 1;", 
			function(err, results){
				// release connection
				connection.release();

				if (err) {
					cb && cb(err, null);
				}
				else{
					results.length ? 
						cb && cb(null, results[0].ver)
						: cb && cb("NO_UPDATE_TOPICS", null);
				}
		});
	});	
};

//
// 查找从指定的版本后的变更记录
// 最多查找一个小时前的变更记录
//
var fetch = function (last_ver, cb) {
	var dbhelper = new Dbhelper(global.DbConnection);
	dbhelper.getConnection(function(err, connection){
		// ver >= last_ver + 1 has
		// better performance than ver > last_ver
		var sql = mysql.format("select * from t_updates where ver >= ? order by ver desc;", 
			[ last_ver+1 ]);

		connection.query(sql, function(err, results){
			// release connection
			connection.release();

			if (err) {
				cb && cb(err, null);
			}
			else{
				results.length ? 
					cb && cb(null, fetchResults(results))	: cb && cb(null, []);
			}
		});
	});	
};

function fetchResults(results) {
	var ret = [];
	results.forEach(function(row){
		ret.push({
			"ver" : row.ver,
			"topic":parseTopic_(row.topic),
			"id":row.id,
			"op":parseOp_(row.op),
			"timestamp":row.ts.valueOf() / 1000
		});
	});

	return ret;
}

function parseOp_ (op) {
	switch(op){
		case 0:
			return "create";
		case 1:
			return "modify";
		case 2:
			return "remove";
		default:
			return op;
	}
}

function parseTopic_ (topic_code) {
	switch(topic_code){
		case 0:
			return "metric";						
		default:
			return topic_code;
	}
}

//
// 注意：
// 	使用该类的前提是需要预先建立dbhelper与mysql的连接
//
function Subscribe (interval) {
	EventEmitter.call(this);

	// signal
	this.do_fetching_ = false;
	this.do_subing_ = false;

	// config
	this.interval_ = interval || 5000;

	// error times
	this.continous_errors_ = 0;
	this.max_err_times_ = 10;

	var self = this;
	this.on('error', self.onErrors);
};

require('util').inherits(Subscribe, EventEmitter);
Subscribe.prototype.constructor = Subscribe;

Subscribe.prototype.sub = function() {
	var self = this;
	if (this.do_subing_)
		return;

	// subpub fetch 时会自动将时间截断到服务器当前时间的 1hour 前
	this.last_version_ = 0;

	lastVersion(function(err, ver){
		if (!err)
			self.last_version_ = ver;

		set_timer();
		self.do_subing_ = true;

		// send signel
		self.emit('ready');
	});

	// set interval timer
	function set_timer(){
		self.timer_ = setInterval(function(){
			self.fetchChanges();
		}, self.interval_);
	}
};

Subscribe.prototype.isSubing = function() {
	return this.do_subing_;
};

Subscribe.prototype.unsub = function() {
	if (this.timer_) {
		clearInterval(this.timer_);
	}
	this.do_subing_ = false;
};

Subscribe.prototype.close = function() {
	this.unsub();
	this.removeAllListeners();
};

Subscribe.prototype.mergeChanges = function(changes) {
	var new_changes = {};

	// NOTE:
	// fetch是按照时间逆序排列的
	//
	changes.forEach(function(change, i){
		var key = [change.topic, change.id, change.op].join('.'); 
		if (!new_changes.hasOwnProperty(key))
			new_changes[key] = change;
	});

	return obj_values(new_changes);
};

Subscribe.prototype.fetchChanges = function() {
	var self = this;
	if (this.do_fetching_)
		return;

	// 考虑到定时器任务可能并行到达，这里避免这种情况
	this.do_fetching_ = true;

	// do fetching
	fetch(this.last_version_, function(err, changes){
		if (err) {
			self.do_fetching_ = false;	// do NOT forget it!
			self.emit("error", new Error(err));
			return;
		}

		// reset continous errors times
		self.continous_errors_ = 0;

		// if NO changes
		if (!changes.length) {
			self.do_fetching_ = false;	// do NOT forget it!
			return;
		}

		// 调用fetch时，服务器已经对结果集进行了排序，因此
		// 如果存在变更，第一条记录一定是last modify version

		// merge changes
		var merged_changes = self.mergeChanges(changes);

		var series = [];
		for(var i=merged_changes.length-1; i>=0; i--) { // fetch是按照时间逆序排列的
			var change = merged_changes[i];
			self.emit(change.op, change.topic, change.id);
			self.last_version_ = change.ver;
		}

		self.do_fetching_ = false;
	});
};

Subscribe.prototype.onErrors = function (err) {
	if ((++this.continous_errors_) > this.max_err_times_){
		// 
		// NOTE:
		//		FATAL ERRORS
		//		MOSTLY, RDMS CONNECTION IS LOST!
		//
		if (logger)
			logger.fatal("OVER MAX CONTINOUS ERRORS, WE HAVE TO LEAVE!");

		// exit the process
		process.exit(1);
	}
};

//
// exports
//

exports.lastVersion = lastVersion;
exports.fetch = fetch;
exports.Subscribe = Subscribe;