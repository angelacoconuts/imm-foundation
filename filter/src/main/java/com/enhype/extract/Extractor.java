package com.enhype.extract;

import java.util.Map;

import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class Extractor {
	
	private static Logger logger = Logger.getLogger(Extractor.class.getName());
	
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
		
		EntityExtractor extractor = new EntityExtractor();
		
		for (String entity : RunConfig.entities)
			extractor.getRelatedEntitiesSite( entity );
		
	}

}
