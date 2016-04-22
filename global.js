var config = require('./config.json');
var mysql = require('mysql');

exports.DbConnection = mysql.createPool(config.db_conn);