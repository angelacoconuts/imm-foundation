package com.enhype.extract;

import java.util.Map;

import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class Extractor {
	
	private static Logger logger = Logger.getLogger(Extractor.class.getName());
	
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
		
		SentenceRanker ranker = new SentenceRanker();
		
		for (String topic : RunConfig.entities)
			ranker.rankSentenceEntity(topic);
		
	}
	
	public static void entractImportantEntities(){
		
		EntityExtractor extractor = new EntityExtractor();
		
		for (String topic : RunConfig.entities)
			extractor.getRelatedEntitiesSite( topic );
		
	}

}
