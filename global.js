var config = require('./config.json');
var mysql = require('mysql');
var App = require('./lib/app');
//
// MySQL connection pool
//
exports.DbConnection = mysql.createPool(config.db_conn);

//
// Application Instance
//
var app_ = new App();
exports.getApp = function (){
	return app_;
}