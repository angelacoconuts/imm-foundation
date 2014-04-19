var selApp = angular.module('selApp', []);

selApp.controller('imgController', function($scope, $http) {
    
    $scope.pictures = [];
    $scope.topics = [];

    $scope.getTopics = function(){
	$http.get('/getimagestopics').then(function(result) {
	    $scope.topics = result.data.topics;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.getImages = function(topic){
	$http.get('/getimages?topic='+topic).then(function(result) {
	    $scope.images = result.data.images;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.selectImage = function(src,index){
	$scope.images[index].selected = true;
	$http.get('/selectimage?src='+src).then(function(result) {
	    console.log("successfully selected image"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.deSelectImage = function(src,index){
	$scope.images[index].selected = false;
	$http.get('/deselectimage?src='+src).then(function(result) {
	    console.log("successfully cancel selection of image"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

});


selApp.controller('quoteController', function($scope, $http) {
    
    $scope.quotes = [];
    $scope.topics = [];

    $scope.getTopics = function(){
	$http.get('/getquotestopics').then(function(result) {
	    $scope.topics = result.data.topics;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.getQuotes = function(topic){
	$http.get('/getquotes?topic='+topic).then(function(result) {
	    $scope.quotes = result.data.quotes;
	    console.log($scope.quotes);
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.selectQuote = function(id,index){
	$scope.quotes[index].selected = true;
	$http.get('/selectquote?id='+id).then(function(result) {
	    console.log("successfully selected quote"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.deSelectQuote = function(src,index){
	$scope.quotes[index].selected = false;
	console.log("deselecting");
	$http.get('/deselectquote?id='+id).then(function(result) {
	    console.log("successfully cancel selection of quote"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

});
