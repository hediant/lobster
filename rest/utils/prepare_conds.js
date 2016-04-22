module.exports = function prepare_conds(query){
	var fields = Object.keys(query);
	var cond  = {};
	fields.forEach(function(field_name){
		query[field_name] ? cond[field_name]=query[field_name] : null;
	});
	return cond;
}

