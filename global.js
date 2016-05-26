var config = require('./config.json');
var mysql = require('mysql');
var App = require('./lib/app');
//
// MySQL connection pool
//
exports.DbConnection = mysql.createPool(config.db_conn);

//
// Environments
//
exports.Env = {
	// main || duplicate
	"mode" : "main"
}

//
// Application Instance
//
var app_ = new App();

//
// initialize and hook process.exit events
//
app_.init();

exports.getApp = function (){
	return app_;
}