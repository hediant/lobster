/*
	val - string, value
	type - string, data type
*/
module.exports = function (val, type){
	switch (type){
		case "String":
			return val;
		case "Digital":
			return parseInt(val);
		case "Analog":
		case "Number":
		default:
			return Number(val);
	}
}