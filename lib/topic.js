var fs = require('fs');
var path = require('path');
var config = require('../config.json');

function Topic (topic_name, metric){
	var me = this;
	var is_ready_ = false;

	this.isReady = function (){
		return is_ready_;
	}
}