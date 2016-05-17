var Q = require('q');

var data;

f1 = Q.Promise(function (resolve, reject){
	data = 1;
	resolve();
})

f2 = Q.Promise(function (resolve, reject){
	d = 2;
	resolve();
})

Q.fcall(f1).then(f2);

function *test (){
	var q=Q.fcall(Q.Promise(function (resolve, reject){resolve();}));

	for (var i=0; i<2; i++){
		q.then(f1);
		yield data;
	}
}

var t=test();
console.log(t.next())
console.log(t.next())