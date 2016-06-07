module.exports = function (array, compare){
	var size = array.length;
	var i, j, compare = compare || function (x, y) {return x > y; };

	for (i=1; i<size; i++){
		var element = array[i];
		for (j=i; j>0 && compare(array[j-1], element) ; j--){
			array[j] = array[j-1];
		}
		array[j] = element;
	}
	
	return array;
}