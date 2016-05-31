function Queue(){
	var me = this;
	var queue_ = [];

	this.next = function (){
		return queue_.shift();
	}

	this.size = function (){
		return queue_.length;
	}

	this.add = function (worker){
		queue_.push(worker);
	}
}

module.exports = Queue;