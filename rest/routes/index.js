var express = require('express');
var router = express.Router();
var G = require('../../global');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Lobster - A Time series data logging server.' });
});

switch (G.Env.mode){
	case "main":
		require("./metrics")(router);
		require("./write")(router);
	case "duplicate":
	default:
		require("./read")(router);
		require('./aggregation')(router);		
}

module.exports = router;

