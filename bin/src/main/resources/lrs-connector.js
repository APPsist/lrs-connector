var vertx   = require('vertx');
var eb      = require('vertx/event_bus');
var console = require('vertx/console');
var container =  require('vertx/container');
var config = container.config;
load('xapiwrapper.min.js');

var lrsconf = config.lrs;
// add eventbus handler
ADL.XAPIWrapper.changeConfig(conf);

var lrsRequestHandler = function(jsonMessage, replyHandler){
	// extract SparQL Query from message
	var statementActor = jsonMessage.sparql.actor;
	var statementVerb = jsonMessage.sparql.verb;
	var statementObject = jsonMessage.sparql.object;
	// TODO: execute query
	lrsStatement(statementActor, statementVerb, statementObject, replyHandler);
	//console.log('request for semantic wiki information arrived:\n'+ jsonMessage);
}

eb.registerHandler("de.appsist.requests.lrs", lrsRequestHandler, function() {
	console.log("LRS Request Handler registered in the cluster");
});

function lrsRequest(statementActor, statementVerb, statementObject, replyHandler) {
	var request = new SPARQL();
	console.log("Storing statement");
	console.log(statementActor+" "+ statementVerb +" "+ statementObject);
	var stmt = new ADL.XAPIStatement(statementActor, statementVerb, statementObject);
	var resp_obj = ADL.XAPIWrapper.sendStatement(stmt);
}

