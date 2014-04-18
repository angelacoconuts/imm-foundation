package com.enhype.photo;

import org.apache.commons.lang3.StringUtils;

import com.enhype.utils.RunConfig;

public class Enricher {
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
	
		FlickrService flickr = new FlickrService();
		
		for (String topic : RunConfig.entities)
			flickr.getCCBySAPhotoFlickr(StringUtils.replace(topic,"_"," "));
		
	}
}
