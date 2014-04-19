var url = require('url');
var postgres = require("./postgresService.js");

exports.getImages = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var resImages = [];
    var queryStr = "select query,img_src,text,entry_point from pictures p,quotes q where p.selected=true and q.selected=true and p.query=q.topic and p.query <> '" + urlQuery.topic + "' order by id;";
    console.log(queryStr);

    postgres.query(queryStr, function(result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    var image = {};
	    image.src = rows[i].img_src;
	    image.title = rows[i].query.replace(/_/g," ");
	    image.text = rows[i].text.substring(0,100);
	    image.site = rows[i].entry_point.substring(7);
	    resImages.push(image);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "images":resImages } ) );
	res.end();

    });

}

exports.getQuotes = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var resQuotes = [];
    var queryStr = "select * from quotes where selected=true and topic = " + "'" + urlQuery.topic + "' order by id asc limit 2;";
    console.log(queryStr);

    postgres.query(queryStr, function(result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    var quote = {};
	    quote.text = rows[i].text;
	    quote.site = rows[i].entry_point.substring(7);
	    resQuotes.push(quote);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "quotes":resQuotes } ) );
	res.end();

    });

}

exports.getFeatures = function (req, res){
    
    var resTopics = [];
    var queryStr = "SELECT iw.feature_word,count(*) FROM IMPORTANT_WORDS iw, word_shortlist ws where iw.feature_word = ws.feature_word group by iw.feature_word ORDER BY count(*) DESC limit 10;";

    postgres.query(queryStr, function (result){

	var rows = result.rows;
	for(var i=0; i< rows.length; i++){
	    var feature = {};
	    feature.word = rows[i].feature_word;
	    feature.count = rows[i].count;
	    resTopics.push(feature);
	}

	res.writeHead( 200 );
	res.write( JSON.stringify( { "features":resTopics } ) );
	res.end();

    });

}
