var prepare_options = require('../../utils/prepare_options');
var response = require('./response');
var G = require('../../global');

module.exports = function (router){
	router.post('/metrics', function (req, res, next) {
		var app = G.getApp();
		app.createMetric(req.body, function (err, metric_id){
			response(res, err, metric_id);
		});
	});

	router.get('/metrics', function (req, res, next){
		var app = G.getApp();
		app.findMetric(req.query, prepare_options(req.query), function (err, results){
			response(res, err, results);
		});
	});

	router.get('/metrics/:metric_name', function (req, res, next){
		var app = G.getApp();
		app.getMetricByName(req.params.metric_name, function (err, metric){
			response(res, err, metric);
		});
	});

	router.put('/metrics/:metric_name', function (req, res, next){
		var app = G.getApp();
		app.setMetricByName(req.params.metric_name, req.body, function (err){
			response(res, err);
		});
	});

	router.delete('/metrics/:metric_name', function (req, res, next){
		var app = G.getApp();
		app.dropMetricByName(req.params.metric_name, function (err){
			response(res, err);
		});
	});
};