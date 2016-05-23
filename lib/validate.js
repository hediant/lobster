exports.topicName = function (topic_name){
	return (typeof topic_name == "string") && (!topic_name.match(/[\/\\\.|<>&\*\?\:\$]+/g))
}