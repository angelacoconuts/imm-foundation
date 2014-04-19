var enhypeApp = angular.module('enhypeApp', []);

enhypeApp.controller('bareBoneController', function($scope, $http) {
    
    $scope.images = [];
    $scope.quotes = [];
    $scope.features = [];
    $scope.topic = 'Hong Kong';

    $scope.getImages = function(topic){
	$http.get('/getimages?topic='+topic.replace(" ","_")).then(function(result) {
	    $scope.images = result.data.images;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.getQuotes = function(topic){
	$http.get('/getquotes?topic='+topic.replace(" ","_")).then(function(result) {
	    $scope.quotes = result.data.quotes;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.getFeatures = function(){
	$http.get('/getfeatures').then(function(result) {
	    $scope.features = result.data.features;
	}, function(error) {
	    console.log(error.message);
	});
    };
    
    $scope.onLoad = function(){
	$scope.getQuotes($scope.topic);
	$scope.getImages($scope.topic);
	$scope.getFeatures();
    }


});

