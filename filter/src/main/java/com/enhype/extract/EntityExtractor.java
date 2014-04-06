package com.enhype.extract;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.amazonaws.util.StringUtils;
import com.enhype.db.PostgresDB;

public class EntityExtractor {
	
	private static Logger logger = Logger.getLogger(EntityExtractor.class.getName());
	private PostgresDB db = new PostgresDB();
	private Map<String, String> entityFormulaMap = new HashMap<String, String>();
	private Map<String, Float> entityScoreMap = new TreeMap<String, Float>();
	
	public Map<String, Float> getRelatedEntities (String queryEntityURI) {
		
		Map<String, Long> relatedEntityAndOccurence = getRelatedEntityAndOccurenceCount (queryEntityURI, 5);		
		
		return downplayCommonEntities(relatedEntityAndOccurence);
		
	}
	
	public Map<String, Float> getRelatedEntitiesSite (String queryEntityURI) {
				
		Map<EntitySiteTuple, Long> occurence = getEntityOccurenceInSite(queryEntityURI, 5);		
		getEntityProminenceInSite(occurence);
		
		for (String entity : entityScoreMap.keySet()){
			String updateStr = "INSERT INTO HK_RELATED_SITE ( ENTITY_URI, SCORE, FORMULA ) "
					+ "VALUES ("
					+ "'" + StringUtils.replace(entity, "'", "''") + "'" + ","
					+ entityScoreMap.get(entity) + ","
					+ "'" + entityFormulaMap.get(entity) + "'"
					+ ") ;";
			
			db.execUpdate(updateStr);
		}
			
		return entityScoreMap;
		
	}
	
	/*
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
	*/
	
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
			
			String queryStr = "select count(*) from entity_mentions re where re.uri = " + "'" + StringUtils.replace(entityURI, "'", "''") + "'";
			logger.debug(queryStr);
			
			long timer = System.currentTimeMillis();
			java.sql.ResultSet result = db.execSelect(queryStr);
			logger.debug( "Time: " + (System.currentTimeMillis() - timer) );

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
	
	
	
	private Map<EntitySiteTuple, Long> getEntityOccurenceInSite (String queryEntityURI, int minOccurrenceThreshold) {
		
		Map<EntitySiteTuple, Long> entityOccurenceMap = new HashMap<EntitySiteTuple, Long>();
		
		String queryStr = "select re.uri, re.site_id, count(*) from entity_mentions e, sentences s, entity_mentions re "
				+ "where e.uri = " + "'" + queryEntityURI + "'"
				+ "and e.mention_sent = s.sent_id "
				+ "and s.sent_id = re.mention_sent "
				+ "group by re.uri, re.site_id; ";	
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				long occurence = (Long) result.getObject("count");
				if( occurence >= minOccurrenceThreshold){
					EntitySiteTuple tuple = new EntitySiteTuple((String) result.getObject("uri"), (String) result.getObject("site_id") );
					entityOccurenceMap.put( tuple , occurence );
				}

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
		return entityOccurenceMap;
		
	}
	
	private void getEntityProminenceInSite (Map<EntitySiteTuple, Long> entityOccurence) {
		
		Map<EntitySiteTuple, Float> entityProminenceMap = new HashMap<EntitySiteTuple, Float>();
		
		long commonality = 0;
		
		for ( EntitySiteTuple entitySite : entityOccurence.keySet() ){
			
			String entity = entitySite.getEntity();
			String queryStr = "select count(*) from entity_mentions re"
					+ " where re.uri = " + "'" + StringUtils.replace(entity, "'", "''") + "'"
					+ " and re.site_id = " + "'" + entitySite.getSiteId() + "'";
			logger.debug(queryStr);
			
			long timer = System.currentTimeMillis();
			java.sql.ResultSet result = db.execSelect(queryStr);
			logger.debug( "Time: " + (System.currentTimeMillis() - timer) );

			try {

				while (result.next()) {

					commonality = (Long) result.getObject("count");
					
					if( commonality > 0){
					
						float score = (float) entityOccurence.get(entitySite) / commonality ; 
						entityProminenceMap.put(entitySite, score );
						
						String formula = entityOccurence.get(entitySite) + "/" + commonality + "; ";
						
						if(entityFormulaMap.containsKey(entity)){
							
							String previous = entityFormulaMap.get(entity);
							entityFormulaMap.put(entity, previous + formula);
							float preScore = entityScoreMap.get(entity);
							entityScoreMap.put(entity, preScore + score);
							
						}else{
							
							entityFormulaMap.put(entity, formula);
							entityScoreMap.put(entity, score);
							
						}
						
					}

				}

			} catch (SQLException ex) {	
				logger.error("SQL Exception: ", ex); 
			} finally {
				db.closeResultSet(result);
			}
			
		}
		
	}
	
}
