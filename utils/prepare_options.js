var escape_array = require('./escape_array');

module.exports = function prepare_options (query) {
	return {
		"sorts" : escape_array(query.sorts),
		"offset" : parseInt(query.offset),
		"limit" : parseInt(query.limit),
		"calc_sum" : (query.calc_sum == "true" ? true : false)
	};	
};