var pg = require('pg');
var url = require('url');
var conString = "postgres://enhype:enhypefun@enhype.cojtnovxyoln.us-west-2.rds.amazonaws.com:5432/enhype";
var client = new pg.Client(conString);

exports.getPictures = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var resPictures = [];
    var queryStr = "select * from pictures where query = " + "'" + urlQuery.topic + "';";

    pg.connect(conString, function(err, client, done) {
	if(err) {
	    return console.error('error fetching client from pool', err);
	}
	client.query(queryStr, function(err, result) {
	    done();
	    if(err) {
		return console.error('error running query', err);
	    }
	    var rows = result.rows;
	    for(var i=0; i< rows.length; i++){
		var picture = {};
		picture.src = rows[i].img_src;
		picture.title = rows[i].img_title;
		picture.selected = rows[i].selected;
		resPictures.push(picture);
	    }

	    res.writeHead( 200 );
            res.write( JSON.stringify( { "pictures":resPictures } ) );
            res.end();

	});
    });

}


exports.selectPicture = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update pictures set selected=true where img_src = " + "'" + urlQuery.src + "';";

    pg.connect(conString, function(err, client, done) {
	if(err) {
	    return console.error('error fetching client from pool', err);
	}
	client.query(queryStr, function(err, result) {
	    done();
	    if(err) {
		return console.error('error running query', err);
	    }

	    res.writeHead( 200 );
	    res.write( JSON.stringify( { "success":1 } ) );
            res.end();

	});
    });

}

exports.deSelectPicture = function (req, res){
    
    var urlQuery = url.parse(req.url, true).query;
    var queryStr = "update pictures set selected=false where img_src = " + "'" + urlQuery.src + "';";

    pg.connect(conString, function(err, client, done) {
	if(err) {
	    return console.error('error fetching client from pool', err);
	}
	client.query(queryStr, function(err, result) {
	    done();
	    if(err) {
		return console.error('error running query', err);
	    }

	    res.writeHead( 200 );
	    res.write( JSON.stringify( { "success":1 } ) );
            res.end();

	});
    });

}

exports.getTopics = function (req, res){
    
    var resTopics = [];
    var queryStr = "select distinct query from pictures;";
    
    pg.connect(conString, function(err, client, done) {
	if(err) {
	    return console.error('error fetching client from pool', err);
	}
	client.query(queryStr, function(err, result) {
	    done();
	    if(err) {
		return console.error('error running query', err);
	    }
	    var rows = result.rows;
	    for(var i=0; i< rows.length; i++){
		resTopics.push(rows[i].query);
	    }

	    res.writeHead( 200 );
            res.write( JSON.stringify( { "topics":resTopics } ) );
            res.end();

	});
    });


}
