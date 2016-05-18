var prepare_options = require('../../utils/prepare_options');
var response = require('./response');
var global = require('../../global');

module.exports = function (router){
	router.post('/metrics', function (req, res, next) {
		var app = global.getApp();
		app.createMetric(req.body, function (err, metric_id){
			response(res, err, metric_id);
		});
	});

	router.get('/metrics', function (req, res, next){
		var app = global.getApp();
		app.findMetric(req.query, prepare_options(req.query), function (err, results){
			response(res, err, results);
		});
	});

	router.get('/metrics/:metric_id', function (req, res, next){
		var app = global.getApp();
		app.getMetric(req.params.metric_id, function (err, metric){
			response(res, err, metric);
		});
	});

	router.put('/metrics/:metric_id', function (req, res, next){
		var app = global.getApp();
		app.setMetric(req.params.metric_id, req.body, function (err){
			response(res, err);
		});
	});

	router.delete('/metrics/:metric_id', function (req, res, next){
		var app = global.getApp();
		app.dropMetric(req.params.metric_id, function (err){
			response(res, err);
		});
	});
};