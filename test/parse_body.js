module.exports = function (body){
	var ret;
	if(typeof body =='string'){
		ret = JSON.parse(body);
	}
	else if(typeof body =='object') {
		if (body)
			ret = body;
		else
			ret = {};
	}

	return ret;	
}