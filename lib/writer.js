var config = require('../config.json');


function Writer(){
	var buffer_ = [];
	var me = this;
	var timer_ = null;

	this.append = function (topic, data, metric){
		if (buffer_.length < config.max_write_buf){
			buffer_.push({
				"topic" : topic,
				"data" : data,
				"metric" : metric
			});

			return;
		}

		me.writeBuffer(buffer_);

		buffer_ = [];
		me.append(topic, data, metric);
	}

	this.writeBuffer = function (buffer){

	}

	this.cycle = function (){
		timer_ = setInterval(function (){
			me.writeBuffer(buffer_);
			buffer_ = [];
		}, config.flush_cycle);
	}

	this.close = function (){
		if (timer_)
			clearInterval(timer_);
		buffer_ = [];
	}

	// begin cycle flush
	this.cycle();
}

module.exports = Writer;