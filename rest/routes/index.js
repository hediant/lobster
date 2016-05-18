var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Lobster - A Time series data logging server.' });
});

require("./metrics")(router);
require("./write")(router);
require("./read")(router);

module.exports = router;

