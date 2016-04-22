var config = require('./config');
var global = require('./global');
var app = require('./rest/app');
var http = require('http');

//////////////////////////////////////////////////////////////////////
/// init services
///
function init(){

    http.createServer(app).listen(config.port, function(){
    	console.log("===================================================");
        console.log("Welcome to lobster!");
        console.log("Lobster is A Time series logging server.");
        console.log("===================================================");
        console.log('PID %s.', process.pid);
        console.log('Server listening on port %s.', config.port);
        console.log('Ready.');
    });
}

init();