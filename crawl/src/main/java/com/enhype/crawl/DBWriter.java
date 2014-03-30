package com.enhype.crawl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.DynamoDB;
import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

public class DBWriter {
	
	private DynamoDB dynamo;
	private Logger logger = Logger.getLogger(DBWriter.class.getName());
	
	private static String entityTableName = "Entities";
	private static String pageTableName = "Pages";
	private static String sentenceTableName = "Sentences";
	private static String entityTableKey = "uri";
	private static String pageTableKey = "id";
	private static String sentenceTableKey = "id";
	
	public DBWriter(){
		
		dynamo = new DynamoDB();

		try {
			DynamoDB.init();
		} catch (Exception e) {
			logger.error("Failing to connection to DynamoDB",e);
		}
		
	}
	
	public void createPageTable(){
		
		long writeCapacity = 5;
		long readCapacity = 10;
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(pageTableKey, "S");
		
		dynamo.createTable(pageTableName, pageTableKey, tableAttrs, readCapacity, writeCapacity );
		
	}
	
	public void createSentenceTable(){
		
		long writeCapacity = RunConfig.SENT_WRITE_CAPACITY;
		long readCapacity = RunConfig.SENT_READ_CAPACITY;
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(sentenceTableKey, "S");
		
		dynamo.createTable(sentenceTableName, sentenceTableKey, tableAttrs, readCapacity, writeCapacity );
		
	}
	
	public void createEntityTable(){
		
		long writeCapacity = RunConfig.ENTITY_WRITE_CAPACITY;
		long readCapacity = RunConfig.ENTITY_READ_CAPACITY;
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(entityTableKey, "S");
		
		dynamo.createTable(entityTableName, entityTableKey, tableAttrs, readCapacity, writeCapacity );
		
	}
	
	public void writePage(WebPage page){
		
		Map<String, String> pageAttrs = new HashMap<String, String>();
		Map<String, List<String>> pageSetAttrs = new HashMap<String, List<String>>();
			
		pageAttrs.put(pageTableKey, page.getId() );
		pageAttrs.put("url", page.getURL());
		pageAttrs.put("title", page.getTitle());
		pageAttrs.put("fetch_time", page.getFetchTime());
		
		if( page.getKeywords() != null && page.getKeywords().length() > 0 )
			pageSetAttrs.put("keywords", Arrays.asList(StringUtils.split(page.getKeywords(),",")));
		
		dynamo.addItem(pageTableName, pageAttrs, pageSetAttrs);
		
	}
	
	public void writeSentences(WebPage page){
			
		logger.info("Writing " + page.getSentencePositionMap().keySet().size() + " sentences. ");
		
			for ( int sentPos : page.getSentencePositionMap().keySet() ){
				
				if( page.getSentenceAdjMap().containsKey(sentPos) || page.getSentenceEntityMap().containsKey(sentPos) ){
					
					Map<String, String> sentAttrs = new HashMap<String, String>();
					Map<String, List<String>> sentSetAttrs = new HashMap<String, List<String>>();
					String sentenceId = page.getId() + "||" + sentPos;
					
					sentAttrs.put(sentenceTableKey, sentenceId);
					sentAttrs.put("text", page.getSentencePositionMap().get(sentPos));
					
					if(page.getSentenceNounMap().containsKey(sentPos))
						sentSetAttrs.put("nouns", new ArrayList(page.getSentenceNounMap().get(sentPos)));
					if(page.getSentenceAdjMap().containsKey(sentPos))
						sentSetAttrs.put("adjs", new ArrayList(page.getSentenceAdjMap().get(sentPos)));
					if(page.getSentenceEntityMap().containsKey(sentPos))
						sentSetAttrs.put("entities", new ArrayList(page.getSentenceEntityMap().get(sentPos)));
					
					long timer = System.currentTimeMillis();
					dynamo.addItem(sentenceTableName, sentAttrs, sentSetAttrs);
					logger.info("Add sentence time: " + (System.currentTimeMillis() - timer) + "ms");
					
				}
				
			}
			
	}
	
	public void writeEntities(WebPage page){
		
		logger.info("Writing " + page.getEntityMentionMap().keySet().size() + " entities. ");
		
		for(String entityURI : page.getEntityMentionMap().keySet()){

			Map<String, List<String>> entityUpdates = new HashMap<String, List<String>>();
			entityUpdates.put("mentions", new ArrayList(page.getEntityMentionMap().get(entityURI)));
			entityUpdates.put("surfaces", new ArrayList(Crawler.entitiesSurfaceList.get(entityURI)));
			
			if(!Crawler.knownEntities.contains(entityURI)){
			
				long timer = System.currentTimeMillis();
				boolean exist = dynamo.doesItemExist(entityTableName, entityTableKey, entityURI);
				long delta_timer = System.currentTimeMillis() - timer;
				logger.info("Check existence time: " + delta_timer + "ms");			
				
				if(!exist){
					
					Map<String, String> entityAttrs = new HashMap<String, String>();
					
					entityAttrs.put(entityTableKey, entityURI);
					if( Crawler.entitiesList.get(entityURI).length() > 0 )
						entityUpdates.put("types", Arrays.asList(StringUtils.split(Crawler.entitiesList.get(entityURI),",")));
					
					dynamo.addItem( entityTableName, entityAttrs, entityUpdates );
					Crawler.knownEntities.add(entityURI);
					
				}
				else{			
					dynamo.addNewValueToItem( entityTableName, entityTableKey, entityURI, entityUpdates );			
				}
				logger.info("Write entity time: " + (System.currentTimeMillis() - timer - delta_timer) + "ms");
				
			}
			else{
				long timer = System.currentTimeMillis();
				dynamo.addNewValueToItem( entityTableName, entityTableKey, entityURI, entityUpdates );
				logger.info("Write entity time: " + (System.currentTimeMillis() - timer) + "ms");
			}

		}
		
	}

}
