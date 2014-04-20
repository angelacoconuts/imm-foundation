package com.enhype.photo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.enhype.utils.RunConfig;

public class Enricher {
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
	
		FlickrService flickr = new FlickrService();
		
		/*
		for (String topic : RunConfig.entities)
			flickr.getCCBySAPhotoFlickr(StringUtils.replace(topic,"_"," "));
		*/
		
		flickr.getOwnerInfoFlickr(flickr.getPhotoOwners());
		
	}
}
