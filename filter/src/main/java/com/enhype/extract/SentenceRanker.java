package com.enhype.extract;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.PostgresDB;

public class SentenceRanker {
	
	private static Logger logger = Logger.getLogger(SentenceRanker.class.getName());
	private PostgresDB db = new PostgresDB();
	private Map<String, Double> entityScoreMap = new HashMap<String, Double>();
	private Map<String, Double> adjectiveScoreMap = new HashMap<String, Double>();
	private Map<String, Double> sentenceEntityScoreMap = new HashMap<String, Double>();
	private Map<String, Double> sentenceAdjectiveScoreMap = new HashMap<String, Double>();
	private Map<String, String> sentenceSet = new HashMap<String, String>();
	private String dbpediaURIPrefix = "http://dbpedia.org/resource/";
	DBBulkInserter dbInsert = new DBBulkInserter();
	
	public void rankSentenceEntity(String topic){
		
		fillEntityScoreMap(topic);
		fillAdjectiveScoreMap(topic);
		
		sentenceSet.clear();
		sentenceEntityScoreMap.clear();
		sentenceAdjectiveScoreMap.clear();
		entityScoreMap.clear();
		adjectiveScoreMap.clear();
		
		String queryStr = "select re.key, re.value, re.sent_id, re.site_id from entity_mentions e, sentence_features re "
				+ "where e.uri = " + "'" + dbpediaURIPrefix + topic + "'"
				+ " and e.mention_sent = re.sent_id"
				+ " and re.key in ('E','A');";	
		
		java.sql.ResultSet result = db.execSelect(queryStr, 100);
		
		try {

			while (result.next()) {
				
				String type = (String) result.getObject("key");
				String feature = (String) result.getObject("value");
				String sentence = (String) result.getObject("sent_id");
				String site = (String) result.getObject("site_id");
				
				sentenceSet.put(sentence, site);
				
				if(type.equals("E") && entityScoreMap.containsKey(feature)){
					
					double score = entityScoreMap.get(feature);
					
					if(score <= 0)
						continue;
					
					logger.debug("Found entity " + feature + "; Score: " + score );
					
					if(sentenceEntityScoreMap.containsKey(sentence))
						sentenceEntityScoreMap.put(sentence, sentenceEntityScoreMap.get(sentence) + score);
					else
						sentenceEntityScoreMap.put(sentence, score);
					
				}
				
				if(type.equals("A") && adjectiveScoreMap.containsKey(feature)){
					
					double score = adjectiveScoreMap.get(feature);
					
					if(score <= 0)
						continue;
					
					logger.debug("Found adjective " + feature + "; Score: " + score );
					
					if(sentenceAdjectiveScoreMap.containsKey(sentence))
						sentenceAdjectiveScoreMap.put(sentence, sentenceAdjectiveScoreMap.get(sentence) + score);
					else
						sentenceAdjectiveScoreMap.put(sentence, score);
					
				}
				
			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
		logger.info("Total Sentence #: " + sentenceSet.keySet().size());
		
		for (String sentence : sentenceSet.keySet()){
			
			double entityScore = 0, adjScore = 0;
			if(sentenceEntityScoreMap.containsKey(sentence))
				entityScore = sentenceEntityScoreMap.get(sentence);
			if(sentenceAdjectiveScoreMap.containsKey(sentence))
				adjScore = sentenceAdjectiveScoreMap.get(sentence);
			
			String updateStr = "INSERT INTO IMPORTANT_SENTENCES_2 ( TOPIC, SENT_ID, SITE_ID, ENTITY_SCORE, ADJECTIVE_SCORE) "
					+ "VALUES ("
					+ "'" + topic + "'" + ","
					+ "'" + sentence + "'" + ","
					+ "'" + sentenceSet.get(sentence) + "'" + ","
					+ entityScore + ","
					+ adjScore
					+ ") ;";
			
			dbInsert.addToQueryListOrExecute(updateStr);
		//	db.execUpdate(updateStr);
		}
		
	}
	
	private void fillEntityScoreMap(String topic) {
		
		String queryStr = "select s.ENTITY_URI, s.SITE_PROMOTION_SCORE"
				+ " from important_topics s"
				+ " where s.PROMINENCE_SCORE > 0.5 and topic= " + "'" + topic + "' ;";	
		
		logger.info("== fillEntityScoreMap ==");
		
		entityScoreMap.clear();
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Fill entity score map time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				String entity = (String) result.getObject("ENTITY_URI");
				double rawScore = (Double) result.getObject("SITE_PROMOTION_SCORE");
				entityScoreMap.put(entity, Math.log10(rawScore));

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
	}
	
	private void fillAdjectiveScoreMap(String topic) {
		
		String queryStr = "select iw.FEATURE_WORD, iw.PROMINENCE_SCORE"
				+ " from important_words iw, word_shortlist ws"
				+ " where iw.feature_word = ws.feature_word"
				+ " and topic= " + "'" + topic + "' ;";	
		
		logger.info("== fillAdjectiveScoreMap ==");

		adjectiveScoreMap.clear();
		
		long timer = System.currentTimeMillis();
		java.sql.ResultSet result = db.execSelect(queryStr);
		logger.info( "Fill adjective score map time: " + (System.currentTimeMillis() - timer) );

		try {

			while (result.next()) {
				
				String adjective = (String) result.getObject("FEATURE_WORD");
				double rawScore = (Double) result.getObject("PROMINENCE_SCORE");
				adjectiveScoreMap.put(adjective, Math.log10(rawScore));

			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
	}

}
