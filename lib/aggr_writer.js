var config = require('../config.json')
	, MongoClient = require('mongodb').MongoClient
	, co = require('co')
	, Q = require('q')
	, region = require('./region');

function AggrWriter(topic_name){
	var me = this;
	var region_idx_ = region(topic_name);

	this.save = function (aggregations, cb){
		if (!cb){
			return Q.Promise((resolve, reject) => {
				me.save(aggregations, function (err){
					err ? reject(err) : resolve();
				});
			});
		}

		var docs = aggregations;
		if (!docs.length)
			return cb();

		var db, col;
		co(function *(){
			db = yield MongoClient.connect(config.mongodb.url);
			col = db.collection(region_idx_.toString());
			yield col.insertMany(docs, {ordered:false});

			db.close();
			cb();
		})
		.catch((err) => {

			if (err.code == 11000){

				co(function *(){
					// var db = yield MongoClient.connect(config.mongodb.url);
					// var col = db.collection(topic_name);
					for (var i=0; i<err.writeErrors.length; i++){
						yield col.save(err.writeErrors[i].getOperation());
					}

					db.close();
					cb();
				})
				.catch((err) => {
					cb(err);
				});
				
			}
			else{
				//
				// Do NOT db.close() here, if do so, mongo client will halt here, but why ?
				//
				cb(err);
			}			
		})
	}
}

module.exports = AggrWriter;