angular.module('imageSelApp', []).

controller('imgController', function($scope, $http) {
    
    $scope.pictures = [];
    $scope.topics = [];

    $scope.getTopics = function(){
	$http.get('/gettopics').then(function(result) {
	    $scope.topics = result.data.topics;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.getPictures = function(topic){
	$http.get('/getpictures?topic='+topic).then(function(result) {
	    $scope.pictures = result.data.pictures;
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.selectPicture = function(src,index){
	$scope.pictures[index].selected = true;
	$http.get('/selectpicture?src='+src).then(function(result) {
	    console.log("successfully selected picture"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

    $scope.deSelectPicture = function(src,index){
	$scope.pictures[index].selected = false;
	$http.get('/deselectpicture?src='+src).then(function(result) {
	    console.log("successfully cancel selection of picture"+src);
	}, function(error) {
	    console.log(error.message);
	});
    };

});
