module.exports = function prepare_fields(query, field_names) {
	var ret_fields_ = {};
	field_names.forEach(function(field_name){
		if (query.hasOwnProperty(field_name)){
			ret_fields_[field_name] = query[field_name];
		}
	});

	return ret_fields_;
};