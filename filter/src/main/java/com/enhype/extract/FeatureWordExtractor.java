package com.enhype.extract;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.PostgresDB;
import com.enhype.utils.DBBulkInserter;

public class FeatureWordExtractor {
	
	private static Logger logger = Logger.getLogger(FeatureWordExtractor.class.getName());
	private Map<String, Integer> siteSentNumMap = new HashMap<String, Integer>();
	private Map<FeatureSiteTuple, Long> adjectiveOccurenceMap = new HashMap<FeatureSiteTuple, Long>();
	private PostgresDB db = new PostgresDB();
	private String dbpediaURIPrefix = "http://dbpedia.org/resource/";
	/*
	private int maximumRequestSize = 100;
	private int currentRequestSize = 0;
	private String query = "";
	*/
	DBBulkInserter dbInsert = new DBBulkInserter();
	
	public void getImportantFeatureWords (String topic) {
					
		Map<FeatureSiteTuple, Long> featureWordOccurence = getFeatureWordOccurenceGroupbySite(dbpediaURIPrefix + topic, 1);	
		
		Map<String, Double> featureWordProminenceScore = logScaleOffsetBySentNumInSite(featureWordOccurence);
		
		logger.info("Total Feature #: " + featureWordProminenceScore.keySet().size());
		
		for (String featureWord : featureWordProminenceScore.keySet()){
			
			String updateStr = "INSERT INTO IMPORTANT_WORDS ( TOPIC, FEATURE_WORD , PROMINENCE_SCORE ) "
					+ "VALUES ("
					+ "'" + StringUtils.replace(topic, "'", "''") + "'" + ","
					+ "'" + StringUtils.replace(featureWord, "'", "''") + "'" + ","
					+ featureWordProminenceScore.get(featureWord)
					+ ") ;";
			
			logger.info(updateStr);
			
		//	query = addToQueryListOrExecute (query , updateStr);
		//	db.execUpdate(updateStr);
			dbInsert.addToQueryListOrExecute(updateStr);
		}
		
	}
	
	/*
	private String addToQueryListOrExecute (String existingQuery , String newRequest) {
		
		String newQuery = existingQuery + " " + newRequest;
		
		if (currentRequestSize < maximumRequestSize - 1){
			
			currentRequestSize++;
			return newQuery;
			
		}else{
			
			db.execUpdate(newQuery);
			currentRequestSize = 0;
			return "";
			
		}
		
	}
	*/
	
	public void fillSiteSentNumMap() {
		
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
	
	public void fillAdjectiveOccurenceMap() {
		
		String queryStr = "select adjective, site_id, count from adjective_occurence;";	
		
		logger.info("== fillAdjectiveOccurenceMap ==");
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				String adjective = (String) result.getObject("adjective");
				String siteId = (String) result.getObject("site_id");
				long occurence = (Long) result.getObject("count");
				
				FeatureSiteTuple tuple = new FeatureSiteTuple( adjective, siteId );
				adjectiveOccurenceMap.put( tuple , occurence );
					
			}
			
			logger.info("Total tuple #: " + adjectiveOccurenceMap.size() ); 

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
	}
	
	private Map<FeatureSiteTuple, Long> getFeatureWordOccurenceGroupbySite (String queryEntityURI, int minOccurrenceThreshold) {
		
		Map<FeatureSiteTuple, Long> featureWordOccurenceMap = new HashMap<FeatureSiteTuple, Long>();
		
		logger.info("== getFeatureAdjNounOccurenceGroupbySite ==");
		String queryStr = "select sf.adjective, sf.site_id, count(*) from sentence_features e, sentence_adjectives sf "
				+ "where e.value = " + "'" + queryEntityURI + "'"
				+ "and e.sent_id = sf.sent_id "
				+ "group by sf.adjective, sf.site_id; ";	
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Select feature word occurence group by site time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				long occurence = (Long) result.getObject("count");
				if( occurence >= minOccurrenceThreshold){
					FeatureSiteTuple tuple = new FeatureSiteTuple((String) result.getObject("adjective"), (String) result.getObject("site_id") );
					featureWordOccurenceMap.put( tuple , occurence );
				}

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
		return featureWordOccurenceMap;
		
	}
	
	private Map<String, Double> logScaleOffsetBySentNumInSite (Map<FeatureSiteTuple, Long> featureWordOccurence) {
		
		Map<String, Double> featureWordScoreMap = new HashMap<String, Double>();		
		Set<FeatureSiteTuple> featureWordPair = featureWordOccurence.keySet();
		// long globalProminence;
		
		logger.info("== logScaleOffsetBySentNumInSite ==");
		logger.info("Starting processing: " + featureWordPair.size() + " feature word - site tuples");
		
		for ( FeatureSiteTuple featureWordSite : featureWordOccurence.keySet() ){
			
			String siteId = featureWordSite.getSiteId();
			String featureWord = featureWordSite.getFeature();
			
			/*
			globalProminence = 0;
			
			if(featureWord == null || siteId == null 
					|| featureWord.length() == 0 || siteId.length() == 0 || !siteSentNumMap.containsKey(siteId) )
				continue;
			
			String queryStr = "select count(*) from sentence_features sf "
					+ "where sf.value = " + "'" + StringUtils.replace(featureWord,"'","''") + "'"
					+ " and sf.site_id = " + "'" + siteId + "';";	

			logger.info(queryStr);
			long timer = System.currentTimeMillis();
			java.sql.ResultSet result = db.execSelect(queryStr);
			logger.info( "Select site time: " + (System.currentTimeMillis() - timer) );

			try {				
				result.next();
				globalProminence = (Long) result.getObject("count");				
			} catch (SQLException ex) {	
				logger.error("SQL Exception: ", ex); 
			} finally {
				if(result != null)
					db.closeResultSet(result);
			}
			
			logger.info("Count:" + globalProminence);
			if(globalProminence == 0)
				continue;
			*/	

			if( !adjectiveOccurenceMap.containsKey(featureWordSite) ){
				logger.info("Site: " + siteId + " ; Word: " + featureWord + " can not be found ");
				continue;
			}
			
			if( !siteSentNumMap.containsKey(siteId) ){
				logger.info("Site: " + siteId + " can not be found ");
				continue;
			}
			
			int siteSentNum = siteSentNumMap.get(siteId);
			long globalProminence = adjectiveOccurenceMap.get(featureWordSite);
		
		//	double score = Math.log10( (double) siteSentNum / globalProminence ) * featureWordOccurence.get(featureWordSite) ; 
			double score = Math.log10( (double) siteSentNum / globalProminence ) * featureWordOccurence.get(featureWordSite);
		//	logger.info("Score:" + score);
			
			if(featureWordScoreMap.containsKey(featureWord)){				
				double preScore = featureWordScoreMap.get(featureWord);
				featureWordScoreMap.put(featureWord, preScore + score);				
			}else{				
				featureWordScoreMap.put(featureWord, score);				
			}
			
		}
		
		return featureWordScoreMap;
		
	}

}
