var config = require('../../config')
	, Q = require('q')
	, co = require('co')
	, AggrIndex = require('../../lib/aggr_index');

function AggrIndexJob(region_idx){
	var me = this;

	this.getName = function (){
		return region_idx.toString();
	}

	this.doWork = function (){
		return Q.Promise((resolve, reject) => {
			co(function *(){
				yield AggrIndex.create(region_idx);
				resolve();
			})
			.catch((err) => {
				reject(err);
			});
		});
	}

	this.onComplited = function (){
		return Q.Promise((resolve, reject) => {
			resolve();		
		})
	}
}

module.exports = AggrIndexJob;