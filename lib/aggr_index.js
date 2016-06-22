var config = require('../config.json')
	, MongoClient = require('mongodb').MongoClient
	, co = require('co')
	, Q = require('q');

var createIndex = function (region_idx){
	return Q.Promise((resolve, reject) => {
		co(function *(){
			var db = yield MongoClient.connect(config.mongodb.url);
			var col = db.collection(region_idx.toString());

			yield col.createIndex({"topic":1, "ts":1});
			resolve();
		})
		.catch ((err) => {
			reject(err);
		});
	});
}

exports.create = createIndex;