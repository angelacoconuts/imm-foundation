var pg = require('pg');
var conString = "postgres://enhype:enhypefun@enhype.cojtnovxyoln.us-west-2.rds.amazonaws.com:5432/enhype";
var client = new pg.Client(conString);

exports.query = function(queryStr, successCallback) {

    pg.connect(conString, function(err, client, done) {

	if(err) {
	    return console.error('error fetching client from pool', err);
	}
	client.query(queryStr, function(err, result) {
	    done();
	    if(err) {
		return console.error('error running query', err);
	    }
	    successCallback(result);

	});
    });

}
