package com.enhype.extract;

import java.util.Map;

import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class Extractor {
	
	private static Logger logger = Logger.getLogger(Extractor.class.getName());
	private static String queryEntityURI = "http://dbpedia.org/resource/Hong_Kong";
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
		EntityExtractor extractor = new EntityExtractor();
		
	//	extractor.getRelatedEntities(queryEntityURI);	
		extractor.getRelatedEntitiesSite(queryEntityURI);
		
	}

}
