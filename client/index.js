var request = require('request')
	, qs = require('querystring')
	, url = require('url')
	, HttpStatus = require('http-status-codes');

/*
	options - object
	{
		"hosts" : [
			e.g. http://host:8001,
			...
		],
		"duplicates" : null || [
			e.g. http://host:8002,
			...
		]
	}
*/
function Client(options){
	var me = this;
	var host_ = options && options.hosts ? options.hosts[0] : "http://localhost:8001";
	var dup_ = options && options.duplicates ? options.duplicates[0] : null;

	var responseStatus_ = function (res, cb){
		switch (res.statusCode){
			case HttpStatus.OK:
			case HttpStatus.NOT_MODIFIED:
				return true;
			default:
				cb && cb(errorMsg_("ER_INTERNAL_ERROR", HttpStatus.getStatusText(res.statusCode)));
				return false;
		}
	}

	var errorMsg_ = function (code, message){
		return {
			"code" : code,
			"message" : message
		}
	}

	var handelBody_ = function (body, cb){
		if (body.err)
			cb && cb(errorMsg_(body.err))
		else
			cb && cb(null, body.ret);			
	}

	//////////////////////////////////////////////////////////
	// metric commands

	/*
		@metric - object
		{
			"name" : "{metric_name}",
			"desc" : "{description}",
			"keys" : [
				{
					"name" : "{key_name}",
					"type" : "Number"||"String"||"Digital"||"Analog"
				},
				...
			]
		}
		@cb - function (err, metric_id - number)

	*/
	this.createMetric = function (metric, cb){
		request.post({
			"url" : host_ + "/metrics",
			"json" : metric
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;
				
			handelBody_(body, cb);	
		});
	}

	/*
		@metric_name - string,
		cb - function (err, metric)
	*/
	this.getMetric = function (metric_name, cb){
		request.get({
			"url" : url.format(host_ + "/metrics/" + metric_name)
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;
				
			handelBody_(JSON.parse(body), cb);
		});
	}

	/*
		@metric - object
		{
			"name" : "{metric_name}",
			"desc" : "{description}",
			"keys" : [
				{
					"name" : "{key_name}",
					"type" : "Number"||"String"||"Digital"||"Analog"
				},
				...
			]
		}
		@cb - function (err)

	*/
	this.setMetric = function (metric, cb){
		request.put({
			"url" : url.format(host_ + "/metrics/" + metric.name),
			"json" : metric
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;		

			cb && cb(body.err? errorMsg_(body.err) : null);			
		});
	}

	/*
		@metric_name - number,
		cb - function (err)
	*/
	this.dropMetric = function (metric_name, cb){
		request.delete({
			"url" : url.format(host_ + "/metrics/" + metric_name)
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;		

			cb && cb(body.err? errorMsg_(body.err) : null);			
		});		
	}

	/*
		@query - object
		{
			"name" : string,
			"desc" : string,
			"limit" : number,
			"sorts" : {string[-]}
			"calc_sum" : true || false
		}
		cb - function (err, results)
		if (calc_sum) 
			results == count of metrics, number
		else
			results == array of metrics
	*/
	this.findMetrics = function (query, cb){
		request.get({
			"url" : url.format(host_ + "/metrics?" + qs.stringify(query))
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;
				
			handelBody_(JSON.parse(body), cb);
		});		
	}

	///////////////////////////////////////////////////////////
	// write commands

	/*
		@queue - array
		[
			{
				"topic" : "{topic_name}",
				"metric" : "{metric_name}",
				"data" : {object},
				"ts" : {timestamp}
			},
			...			
		],
		@cb - function (err, ret)
		if (err == "ER_PARTIAL_FAIL") 
			ret - array of reason(string)
		else 
			ret - null
	*/
	this.append = function (queue, cb){
		request.post({
			"url" : host_ + "/append",
			"json" : queue
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;
				
			handelBody_(body, cb);
		})
	}

	/////////////////////////////////////////////////////////////////////
	// read commands

	/*
		@topic_name - string
		@query - object
		{
			fields : array of string,
			start : timestamp,
			end : timestamp,
			limit : number
		}
		@cb - function (err, results)
			err - null || reason
			results - object
			{
				"{field_name}" : [
					[{timestamp}, {value}],
					[{timestamp}, {value}],
					[{timestamp}, {value}],
					......
				],
				"{field_name}" : [...],
				......
			}
	*/
	this.readRaw = function (topic_name, query, cb){
		var host = dup_ ? dup_ : host_;
		var query_obj = {}
		for (var key in query){
			if (key == "fields"){
				query_obj["tag"] = query["fields"]
			}
			else
				query_obj[key] = query[key];
		}

		request.get({
			"url" : url.format(host + "/topics/" + topic_name + "/readraw?" + qs.stringify(query_obj))
		}, function (err, res, body){
			if (err)
				return cb && cb(errorMsg_("ER_SERVICE_NOT_AVAILABLE"));			
			if (!responseStatus_(res, cb))
				return;
				
			handelBody_(JSON.parse(body), cb);
		})
	}

	this.readRawStream = function (topic_name, query){
		var host = dup_ ? dup_ : host_;
		return request(url.format(host + "/topics/" + topic_name + "/readraw?" + qs.stringify(query)));
	}		

}

module.exports = Client;