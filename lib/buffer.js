function Buffer(){
	var me = this;
	var buffer_ = {};
	var topics_ =[];
	var len_ = 0;

	this.size = function (){
		return len_;
	}

	this.push = function (topic, data, metric){
		var topic_name = topic.getTopicName();
		var kv = buffer_[topic_name];
		if (!kv){
			buffer_[topic_name] = [];
			kv = buffer_[topic_name];
			topics_.push(topic);
		}

		kv.push({
			"data" : data,
			"metric" : metric,
			"ts" : new Date()
		});

		len_ ++;
	}

	this.shift = function (){
		var topic = topics_.shift();
		if (!topic)
			return null;

		var topic_name = topic.getTopicName();
		var series = buffer_[topic_name];

		delete buffer_[topic_name];
		len_ = len_ - series.length;

		return {
			"topic" : topic,
			"series" : series
		}
	}

	this.clear = function (){
		buffer_ = {};
		topics_ = [];
		len_ = 0;
	}
}

module.exports = Buffer;