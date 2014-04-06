package com.enhype.extract;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.PostgresDB;

public class EntityExtractor {
	
	private static Logger logger = Logger.getLogger(EntityExtractor.class.getName());
	private PostgresDB db = new PostgresDB();
	
	public Map<String, Float> getRelatedEntities (String queryEntityURI) {
		
	//	Map<String, Long> relatedEntityAndOccurence = getRelatedEntityAndOccurenceCount (queryEntityURI, 5);		
		Map<String, Long> relatedEntityAndOccurence = new HashMap<String, Long>();
		try {
			relatedEntityAndOccurence = getRelatedEntityAndOccurenceCountFile (new FileReader("src/main/resources/test_hk"), 5);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return downplayCommonEntities(relatedEntityAndOccurence);
		
	}
	
	private Map<String, Long> getRelatedEntityAndOccurenceCountFile (FileReader occurrenceFile, int minOccurrenceThreshold) {
		
		Map<String, Long> relatedEntityAndOccurenceMap = new HashMap<String, Long>();
		
		BufferedReader br = new BufferedReader(occurrenceFile);
	    try {
	        String line = br.readLine();

	        while (line != null) {
	        	int com = StringUtils.lastIndexOf(line, ',');
	        	if(com > 0){
	        		String entity = StringUtils.substring(line, 0, com);
	        		long count = Long.valueOf(StringUtils.substring(line, com + 1));
	        		if (count >= minOccurrenceThreshold)
	        			relatedEntityAndOccurenceMap.put(entity, count);
	        	}
	            line = br.readLine();
	        }

	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
	        try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
		
		return relatedEntityAndOccurenceMap;
		
	}
	
	private Map<String, Long> getRelatedEntityAndOccurenceCount (String queryEntityURI, int minOccurrenceThreshold) {
		
		Map<String, Long> relatedEntityAndOccurenceMap = new HashMap<String, Long>();
		
		String queryStr = "select re.uri, count(*) from entity_mentions e, sentences s, entity_mentions re "
				+ "where e.uri = " + "'" + queryEntityURI + "'"
				+ "and e.mention_sent = s.sent_id "
				+ "and s.sent_id = re.mention_sent "
				+ "group by re.uri; ";	
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				long occurence = (Long) result.getObject("count");
				if( occurence >= minOccurrenceThreshold)				
					relatedEntityAndOccurenceMap.put( (String) result.getObject("uri") , occurence );

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
		return relatedEntityAndOccurenceMap;
		
	}

	private Map<String, Float> downplayCommonEntities (Map<String, Long> entityAndOccurenceCount) {
		
		Map<String, Float> entitiesAndImportanceMap = new TreeMap<String, Float>();
		
		long commonality = 0;
		
		for ( String entityURI : entityAndOccurenceCount.keySet() ){
			
			String queryStr = "select count(*) from entity_mentions re where re.uri = " + entityURI;
			logger.info(queryStr);
			
			long timer = System.currentTimeMillis();
			java.sql.ResultSet result = db.execSelect(queryStr);
			logger.info( "Time: " + (System.currentTimeMillis() - timer) );

			try {

				while (result.next()) {

					commonality = (Long) result.getObject("count");
					
					if( commonality > 0){
					
						float score = (float) entityAndOccurenceCount.get(entityURI) / commonality ; 
						entitiesAndImportanceMap.put(entityURI, score );
						
						String updateStr = "INSERT INTO HK_RELATED ( ENTITY_URI, SCORE, FORMULA ) "
								+ "VALUES ("
								+ entityURI + ","
								+ entitiesAndImportanceMap.get(entityURI) + ","
								+ "'" + entityAndOccurenceCount.get(entityURI)  + "/" +  commonality + "'"
								+ ") ;";
						
						db.execUpdate(updateStr);
						
					}

				}

			} catch (SQLException ex) {	
				logger.error("SQL Exception: ", ex); 
			} finally {
				db.closeResultSet(result);
			}
			
		}
		
		return entitiesAndImportanceMap;
		
	}
	
}
