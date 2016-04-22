var express = require('express');
var metric = require('../../lib/metric');
var prepare_options = require('../utils/prepare_options');

module.exports = function (router){

	var response = function (res, err, results){
		res.json({
			"err" : err,
			"ret" : results
		});
	}

	router.post('/metrics', function (req, res, next) {
		metric.create(req.body, function (err, metric_id){
			response(res, err, metric_id);
		});
	});

	router.get('/metrics', function (req, res, next){
		metric.find(req.query, prepare_options(req.query), function (err, results){
			response(res, err, results);
		});
	});

	router.get('/metrics/:metric_id', function (req, res, next){
		metric.get(req.params.metric_id, function (err, metric){
			response(res, err, metric);
		});
	});

	router.put('/metrics/:metric_id', function (req, res, next){
		metric.set(req.params.metric_id, req.body, function (err){
			response(res, err);
		});
	});

	router.delete('/metrics/:metric_id', function (req, res, next){
		metric.drop(req.params.metric_id, function (err){
			response(res, err);
		});
	});
};