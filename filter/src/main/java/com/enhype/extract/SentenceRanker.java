package com.enhype.extract;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.PostgresDB;

public class SentenceRanker {
	
	private static Logger logger = Logger.getLogger(SentenceRanker.class.getName());
	private PostgresDB db = new PostgresDB();
	private Map<String, Double> entityScoreMap = new HashMap<String, Double>();
	private Map<String, Double> sentenceScoreMap = new HashMap<String, Double>();
	private String dbpediaURIPrefix = "http://dbpedia.org/resource/";
	
	public void rankSentenceEntity(String topic){
		
		fillEntityScoreMap(topic);
		
		String queryStr = "select re.uri, re.mention_sent from entity_mentions e, sentences s, entity_mentions re "
				+ "where e.uri = " + "'" + dbpediaURIPrefix + topic + "'"
				+ "and e.mention_sent = s.sent_id "
				+ "and s.sent_id = re.mention_sent;";	
		
		java.sql.ResultSet result = db.execSelect(queryStr, 100);
		
		try {

			while (result.next()) {
				
				String entity = (String) result.getObject("uri");
				String sentence = (String) result.getObject("mention_sent");
				
				if(!entityScoreMap.containsKey(entity))
					continue;
				
				double score = entityScoreMap.get(entity);
				
				if(score <= 0)
					continue;
				
				logger.info("Found entity " + entity + "; Score: " + score );
				
				if(sentenceScoreMap.containsKey(sentence))
					sentenceScoreMap.put(sentence, sentenceScoreMap.get(sentence) + score);
				else
					sentenceScoreMap.put(sentence, score);
				
			}

		} catch (SQLException ex) {	
			logger.error("SQL Exception: ", ex); 
		} finally {
			db.closeResultSet(result);
		}
		
		logger.info("Total Sentence #: " + sentenceScoreMap.keySet().size());
		
		for (String sentence : sentenceScoreMap.keySet()){
			
			String updateStr = "INSERT INTO IMPORTANT_SENTENCES ( TOPIC, SENT_ID, SCORE) "
					+ "VALUES ("
					+ "'" + topic + "'" + ","
					+ "'" + sentence + "'" + ","
					+ sentenceScoreMap.get(sentence)
					+ ") ;";
			
			db.execUpdate(updateStr);
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

}
