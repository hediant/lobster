/*
	val - string, value
	type - string, data type
*/
module.exports = function (val, type){
	if (val == undefined || val == null || val == NaN)
		return val;

	switch (type){
		case "String":
			return String(val);
		case "Switch":
			return Boolean(val);
		case "Digital":
			return parseInt(val);
		case "Analog":
		case "Number":
		default:
			return Number(val);
	}
}