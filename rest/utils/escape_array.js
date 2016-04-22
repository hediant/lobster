module.exports = function escape_array(obj) {
	if (typeof obj === "undefined" || obj == null )
		return null;
	if (Array.isArray(obj))
		return obj;
	return [obj];
}