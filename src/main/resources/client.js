var vertx   = require('vertx');
var eb      = require('vertx/event_bus');
var console = require('vertx/console');

var handler_name = 'sparql.address.' + Math.random().toString(36).substring(7);

eb.registerHandler(handler_name, function(message) {
	console.log('Received news ' + message);
}, function(){
	console.log('Yippee! The handler info has been propagated across the cluster');
	request_sparql(handler_name);
});


function request_sparql(handler_name) {

	var client = vertx.createHttpClient()
		.port(1234)
		.host("localhost")
		.keepAlive(false);

	client.exceptionHandler(function(ex) {
		console.error('Oops. Something went wrong ' + ex.getMessage());
	});

	var request = client.get("/handler/" + handler_name, function(resp) {
		console.log("Got a response: " + resp.statusCode());
		resp.dataHandler(function(buffer) {
			console.log('I received ' + buffer.length() + ' bytes');
			console.log(buffer.toString());
		});
	}).end();

	client.close();

}
