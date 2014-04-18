package com.enhype.extract;

import java.util.Map;

import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class Extractor {
	
	private static Logger logger = Logger.getLogger(Extractor.class.getName());
	
	
	public static void main( String[] args ){
		
		RunConfig.parseCfgFromFile("src/main/resources/config.json");
		
		entractImportantWords();
		
	}
	
	public static void entractImportantEntities(){
		
		EntityExtractor extractor = new EntityExtractor();
		
		for (String topic : RunConfig.entities)
			extractor.getRelatedEntitiesSite( topic );
		
	}
	
	public static void entractImportantWords(){
		
		FeatureWordExtractor extractor = new FeatureWordExtractor();
		
		extractor.fillSiteSentNumMap();
		extractor.fillAdjectiveOccurenceMap();
		
		//extractor.getImportantFeatureWords("Xinjiang");
		for (String topic : RunConfig.entities)
			extractor.getImportantFeatureWords(topic);
		
	}
	
	public static void rankSentences(){
		
		SentenceRanker ranker = new SentenceRanker();
		
		for (String topic : RunConfig.entities)
			ranker.rankSentenceEntity(topic);
		
	}

}
