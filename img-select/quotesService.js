var url = require('url');
var postgres = require("./postgresService.js");

exports.getQuotes = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var resQuotes = [];
    var queryStr = "select * from quotes where topic = " + "'" + urlQuery.topic + "';";

    postgres.query(queryStr, function(result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    var quote = {};
	    quote.id = rows[i].id;
	    quote.text = rows[i].text;
	    quote.site = rows[i].entry_point;
	    quote.selected = rows[i].selected;
	    resQuotes.push(quote);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "quotes":resQuotes } ) );
	res.end();

    });

}

exports.selectQuote = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update quotes set selected=true where id = " + "'" + urlQuery.id + "';";

    postgres.query(queryStr,function(result){


	res.writeHead( 200 );
	res.write( JSON.stringify( { "success":1 } ) );
	res.end();
    });
}

exports.deSelectQuote = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update quotes set selected=false where id = " + "'" + urlQuery.id + "';";

    postgres.query(queryStr,function(result){

	res.writeHead( 200 );
	res.write( JSON.stringify( { "success":1 } ) );
	res.end();

    });

}

exports.getTopics = function (req, res){
    
    var resTopics = [];
    var queryStr = "select distinct topic from quotes;";

    postgres.query(queryStr, function (result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    resTopics.push(rows[i].topic);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "topics":resTopics } ) );
	res.end();

    });

}
