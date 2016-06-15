function obj_values(obj) {
	var arr = [];
	for(var key in obj)
		arr.push(obj[key]);

	return arr;
}

module.exports = obj_values;