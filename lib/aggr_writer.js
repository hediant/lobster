var config = require('../config.json')
	, MongoClient = require('mongodb').MongoClient
	, co = require('co')
	, Q = require('q');

function AggrWriter(topic_name){
	var me = this;

	var toDocs_ = function (aggregations){
		var docs = [];
		for (var time in aggregations){
			var doc = { _id:time };
			var record = aggregations[time];

			for (var field_name in record){
				doc[field_name] = record[field_name];
			}

			docs.push(doc);
		}

		return docs;
	}

	this.save = function (aggregations, cb){
		if (!cb){
			return Q.Promise((resolve, reject) => {
				me.save(aggregations, function (err){
					err ? reject(err) : resolve();
				});
			});
		}

		var docs = toDocs_(aggregations);
		if (!docs.length)
			return cb();

		var db, col;
		co(function *(){
			db = yield MongoClient.connect(config.mongodb.url, {db: { bufferMaxEntries: 0 }});
			col = db.collection(topic_name);
			yield col.insertMany(docs, {ordered:false});

			db.close();
			cb();
		})
		.catch((err) => {
			if (err.code == 11000){

				co(function *(){
					for (var i=0; i<err.writeErrors.length; i++){
						yield col.save(err.writeErrors[i].getOperation());
					}

					db.close();
					cb();
				})
				.catch((err) => {
					db.close();
					cb(err);
				});
				
			}
			else{
				db.close();
				cb(err);
			}			
		})
	}
}

module.exports = AggrWriter;