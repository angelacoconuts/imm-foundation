/*
 * GET home page.
 */

exports.images = function(req, res){
  res.render('images', { title: 'Image selector' });
};

exports.quotes = function(req, res){
  res.render('quotes', { title: 'Quote selector' });
};
