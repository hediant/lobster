var g = require('../../global')
	, response = require('./response')
	, escape_array = require('../../utils/escape_array');

module.exports = function (router){
	router.post("/append", function (req, res, next){
		g.getApp().appendBatch(req.body, function (err, results){
			response(res, err, results);
		});
	});

	router.post("/topics/:topic_name/append", function (req, res, next){
		g.getApp().append(req.params.topic_name, req.body, req.query.metric, req.query.ts, function (err, results){
			response(res, err, null);
		});
	})
}