package com.enhype.extract;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.PostgresDB;

public class EntityExtractor {
	
	private static Logger logger = Logger.getLogger(EntityExtractor.class.getName());
	private PostgresDB db = new PostgresDB();
	private Map<String, String> entityFormulaMap = new HashMap<String, String>();
	private Map<String, Integer> siteSentNumMap = new HashMap<String, Integer>();
	private String dbpediaURIPrefix = "http://dbpedia.org/resource/";
		
	public Map<String, Double> getRelatedEntities (String entityRaw) {
		
		Map<String, Long> relatedEntityAndOccurence = getEntityOccurence (dbpediaURIPrefix + entityRaw, 5);		
		
		return dividedByEntityOccurenceInCorpus(relatedEntityAndOccurence);
		
	}
	
	public void getRelatedEntitiesSite (String entityRaw) {
		
		fillSiteSentNumMap();
		
		Map<String, Long> entityAbsoluteOccurence = getEntityOccurence (dbpediaURIPrefix + entityRaw, 5);
		
		Map<String, Double> entityRelavantFrequency = dividedByEntityOccurenceInCorpus(entityAbsoluteOccurence);
				
		Map<FeatureSiteTuple, Long> entitySiteOccurence = getEntityOccurenceGroupbySite(dbpediaURIPrefix + entityRaw, 1);	
		
		Map<String, Double> entityProminenceScore = logScaleOffsetBySentNumInSite(entitySiteOccurence);
		
		logger.info("Total Entity #: " + entityRelavantFrequency.keySet().size());
		
		for (String entity : entityRelavantFrequency.keySet()){
			
			if(!entityProminenceScore.containsKey(entity))
				continue;
			
			String updateStr = "INSERT INTO IMPORTANT_TOPICS ( TOPIC, ENTITY_URI, PROMINENCE_SCORE , SITE_PROMOTION_SCORE, FORMULA ) "
					+ "VALUES ("
					+ "'" + StringUtils.replace(entityRaw, "'", "''") + "'" + ","
					+ "'" + StringUtils.replace(entity, "'", "''") + "'" + ","
					+ entityRelavantFrequency.get(entity) + ","
					+ entityProminenceScore.get(entity) + ","
					+ "'" + entityFormulaMap.get(entity) + "'"
					+ ") ;";
			
			db.execUpdate(updateStr);
		}
		
	}
	
	private void fillSiteSentNumMap() {
		
		String queryStr = "select s.site_id, s.sent_num from sites s;";	
		
		logger.info("== fillSiteSentNumMap ==");
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				int occurence = (Integer) result.getObject("sent_num");
				String entity = (String) result.getObject("site_id");
				siteSentNumMap.put(entity, occurence);

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
	}
	
	private Map<String, Long> getEntityOccurence (String queryEntityURI, int minOccurrenceThreshold) {
		
		Map<String, Long> relatedEntityAndOccurenceMap = new HashMap<String, Long>();
		
		logger.info("== getEntityOccurence ==");
		
		String queryStr = "select re.uri, count(*) from entity_mentions e, sentences s, entity_mentions re "
				+ "where e.uri = " + "'" + queryEntityURI + "'"
				+ "and e.mention_sent = s.sent_id "
				+ "and s.sent_id = re.mention_sent "
				+ "group by re.uri; ";	
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Select entity local occurence time: " + (System.currentTimeMillis() - timer) );
		
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

	private Map<String, Double> dividedByEntityOccurenceInCorpus (Map<String, Long> entityAbsoluteOccurence) {
		
		Map<String, Double> entitiesAndImportanceMap = new HashMap<String, Double>();
		
		long commonality = 0;
		
		logger.info("== dividedByEntityOccurenceInCorpus ==");
		
		for ( String entityURI : entityAbsoluteOccurence.keySet() ){
			
			String queryStr = "select count(*) from entity_mentions re where re.uri = " + "'" + StringUtils.replace(entityURI, "'", "''") + "'";
			logger.debug(queryStr);
			
			long timer = System.currentTimeMillis();
			java.sql.ResultSet result = db.execSelect(queryStr);
			logger.info( "Select entity " + entityURI + " global occurence time: " + (System.currentTimeMillis() - timer) );

			try {

				while (result.next()) {

					commonality = (Long) result.getObject("count");
					
					if( commonality > 0){
					
						double score = (double) entityAbsoluteOccurence.get(entityURI) / commonality ; 
						entitiesAndImportanceMap.put(entityURI, score );
						entityFormulaMap.put( entityURI, "(" + entityAbsoluteOccurence.get(entityURI)  + "/" +  commonality + ");; " );
						
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
	
	
	
	private Map<FeatureSiteTuple, Long> getEntityOccurenceGroupbySite (String queryEntityURI, int minOccurrenceThreshold) {
		
		Map<FeatureSiteTuple, Long> entityOccurenceMap = new HashMap<FeatureSiteTuple, Long>();
		
		logger.info("== getEntityOccurenceGroupbySite ==");
		String queryStr = "select re.uri, re.site_id, count(*) from entity_mentions e, sentences s, entity_mentions re "
				+ "where e.uri = " + "'" + queryEntityURI + "'"
				+ "and e.mention_sent = s.sent_id "
				+ "and s.sent_id = re.mention_sent "
				+ "group by re.uri, re.site_id; ";	
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Select entity occurence group by site time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				long occurence = (Long) result.getObject("count");
				if( occurence >= minOccurrenceThreshold){
					FeatureSiteTuple tuple = new FeatureSiteTuple((String) result.getObject("uri"), (String) result.getObject("site_id") );
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
	
	private Map<String, Double> logScaleOffsetBySentNumInSite (Map<FeatureSiteTuple, Long> entityOccurence) {
		
		Map<String, Double> entityScoreMap = new HashMap<String, Double>();		
		Set<FeatureSiteTuple> entityPair = entityOccurence.keySet();
		
		logger.info("== logScaleOffsetBySentNumInSite ==");
		logger.info("Starting processing: " + entityPair.size() + " entity site tuples");
		
		for ( FeatureSiteTuple entitySite : entityOccurence.keySet() ){
			
			String siteId = entitySite.getSiteId();
			String entityURI = entitySite.getFeature();
			
			if(entityURI == null || siteId == null 
					|| entityURI.length() == 0 || siteId.length() == 0 || !siteSentNumMap.containsKey(siteId) )
				continue;
			
			int siteSentNum = siteSentNumMap.get(siteId);

			String formula = "log(" + siteSentNum + "/" + entityOccurence.get(entitySite)  +  ") + ";
			logger.debug("Fomula:" + formula);
		
			double score = Math.log10( (double) siteSentNum / entityOccurence.get(entitySite) ) ; 
			logger.debug("Score:" + score);
			
			if(entityFormulaMap.containsKey(entityURI)){				
				String previous = entityFormulaMap.get(entityURI);
				entityFormulaMap.put(entityURI, previous + formula);				
			}else{				
				entityFormulaMap.put(entityURI, formula);				
			}
			
			if(entityScoreMap.containsKey(entityURI)){				
				double preScore = entityScoreMap.get(entityURI);
				entityScoreMap.put(entityURI, preScore + score);				
			}else{				
				entityScoreMap.put(entityURI, score);				
			}
			
		}
		
		return entityScoreMap;
		
	}
	
}
