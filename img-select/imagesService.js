var url = require('url');
var postgres = require("./postgresService.js");

exports.getImages = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var resImages = [];
    var queryStr = "select * from pictures where query = " + "'" + urlQuery.topic + "';";

    postgres.query(queryStr, function(result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    var image = {};
	    image.src = rows[i].img_src;
	    image.title = rows[i].img_title;
	    image.selected = rows[i].selected;
	    resImages.push(image);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "images":resImages } ) );
	res.end();

    });

}

exports.selectImage = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update pictures set selected=true where img_src = " + "'" + urlQuery.src + "';";

    postgres.query(queryStr,function(result){


	res.writeHead( 200 );
	res.write( JSON.stringify( { "success":1 } ) );
	res.end();
    });
}

exports.deSelectImage = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update pictures set selected=false where img_src = " + "'" + urlQuery.src + "';";

    postgres.query(queryStr,function(result){

	res.writeHead( 200 );
	res.write( JSON.stringify( { "success":1 } ) );
	res.end();

    });

}

exports.getTopics = function (req, res){
    
    var resTopics = [];
    var queryStr = "select distinct query from pictures;";

    postgres.query(queryStr, function (result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    resTopics.push(rows[i].query);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "topics":resTopics } ) );
	res.end();

    });

}
