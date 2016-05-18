module.exports = function (res, err, results){
	res.json({
		"err" : err,
		"ret" : results
	});
}