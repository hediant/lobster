module.exports = function (val){
	return val == null || val == undefined || val === "" || val === NaN;
}