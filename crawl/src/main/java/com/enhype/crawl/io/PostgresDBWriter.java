package com.enhype.crawl.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.crawl.Crawler;
import com.enhype.db.PostgresDB;
import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

public class PostgresDBWriter {
	
	private PostgresDB postgres;
	private Logger logger = Logger.getLogger(PostgresDBWriter.class.getName());
	private int maximumRequestSize = 500;
	private int currentRequestSize = 0;
	private String query = "";
	
	public PostgresDBWriter(){		
		postgres = new PostgresDB();		
	}
	
	public void dropAllTables(){
		
		String dropTables = 
				"DROP TABLE SITES; "
				+ "DROP TABLE PAGES; "
				+ "DROP TABLE SENTENCES; "
				+ "DROP TABLE SENTENCE_FEATURES; "
				+ "DROP TABLE ENTITY_MENTIONS; ";
		
		postgres.execUpdate(dropTables);
		
		
	}
	
	public void createSiteTable() {
		
		String createSiteTable = "CREATE TABLE SITES ( "
				+ "SITE_ID VARCHAR(15), "         //[MACHINE_ID]||S[SITE_SEQ] e.g: 1||S1, 10||S29
				+ "ENTRY_POINT VARCHAR(255), "
				+ "START_PAGE_ID VARCHAR(20), "
				+ "END_PAGE_ID VARCHAR(20), "
				+ "SENT_NUM INT "
				+ ");";

		postgres.execUpdate( createSiteTable );

	}
	
	public void createPageTable() {

		String createPageTable = "CREATE TABLE PAGES ( "
				+ "PAGE_ID VARCHAR(20), "         //[MACHINE_ID]||P[PAGE_SEQ] e.g: 3||P124, 8||P16
				+ "URL VARCHAR(511), "
				+ "TITLE VARCHAR(255), "
				+ "KEYWORDS VARCHAR(255), "
				+ "FETCH_TIME TIMESTAMP "
				+ ");";

		postgres.execUpdate( createPageTable );

	}
	
	public void createSentenceTable() {

		String createSentenceTable = "CREATE TABLE SENTENCES ( "
				+ "SENT_ID VARCHAR(25), "         //[MACHINE_ID]||P[PAGE_SEQ]||[SENT_POS] e.g: 3||P124||1024, 8||P16||2370
				+ "TEXT VARCHAR(1023), "
				+ "DOMAIN INT "
				+ ");";
		
		String createSentenceFeatureTable = "CREATE TABLE SENTENCE_FEATURES ( "
				+ "SENT_ID VARCHAR(25), "         //[MACHINE_ID]||P[PAGE_SEQ]||[SENT_POS] e.g: 3||P124||1024, 8||P16||2370
				+ "KEY VARCHAR(1), "			  //A=Adjective, N=Noun, E=Entity
				+ "VALUE VARCHAR(511), "
				+ "SITE_ID VARCHAR(15) "
				+ ");";

		postgres.execUpdate( createSentenceTable );
		postgres.execUpdate( createSentenceFeatureTable );

	}

	public void createEntityTable() {

		/*
		String createEntityTable = "CREATE TABLE ENTITIES ( "
				+ "URI VARCHAR(511), "         
				+ "TYPES VARCHAR(255), "
				+ "BUCKET INT "
				+ ");";
		*/
		
		String createEntityMentionTable = "CREATE TABLE ENTITY_MENTIONS ( "
				+ "URI VARCHAR(511), "    
				+ "MENTION_SENT VARCHAR(25), "     //[MACHINE_ID]||P[PAGE_SEQ]||[SENT_POS] e.g: 3||P124||1024, 8||P16||2370
				+ "SURFACE VARCHAR(255), "
				+ "SITE_ID VARCHAR(15) "
				+ ");";

		// postgres.execUpdate( createEntityTable );
		postgres.execUpdate( createEntityMentionTable );

	}

	public void writeSite(String entry, long startPageSeq, long endPageSeq, long sentSeq) {
		
		String insertSite = "INSERT INTO SITES ( "
				+ "SITE_ID, "
				+ "ENTRY_POINT, "
				+ "START_PAGE_ID, "
				+ "END_PAGE_ID, "
				+ "SENT_NUM "
				+ ") "
				+ "VALUES ( "
				+ "'" + SeqGenerator.getSiteId() + "',"
				+ "'" + entry + "',"
				+ "'" + SeqGenerator.getPageId(startPageSeq) + "',"
				+ "'" + SeqGenerator.getPageId(endPageSeq) + "',"
				+ sentSeq
				+ " );";

		postgres.execUpdate( insertSite );

	}
	
	public void writePage(WebPage page) {
		
		String insertSite = "INSERT INTO PAGES ( "
				+ "PAGE_ID, "
				+ "URL, "
				+ "TITLE, "
				+ "KEYWORDS, "
				+ "FETCH_TIME "
				+ ") "
				+ "VALUES ( "
				+ "'" + page.getId() + "',"
				+ "'" + StringUtils.replace(StringUtils.substring(page.getURL(), 0, 505), "'", "''") + "',"
				+ "'" + StringUtils.replace(StringUtils.substring(page.getTitle(), 0, 250), "'", "''") + "',"
				+ "'" + StringUtils.replace(StringUtils.substring(page.getKeywords(), 0, 250), "'", "''") + "',"
				+ "'" + page.getFetchTime() + "'"
				+ " );";

		postgres.execUpdate( insertSite );

	}


	public void writeSentences(WebPage page) {	
		
		logger.info("Writing " + page.getSentencePositionMap().keySet().size()
				+ " sentences. ");
		
		List<Integer> sents = new ArrayList(page.getSentencePositionMap().keySet());
				
		for ( int k = 0 ; k < sents.size() ; k++ ) {
			
			int pos = sents.get(k);
			String sentId = SeqGenerator.getSentId(page.getId(), pos);
			
			String singleSentInsert = "INSERT INTO SENTENCES ( "
					+ "SENT_ID, "
					+ "TEXT, "
					+ "DOMAIN "
					+ ") "
					+ "VALUES ( "
					+ "'" + sentId + "',"
					+ "'" + StringUtils.replace(StringUtils.substring(page.getSentencePositionMap().get(pos), 0, 1000), "'", "''") + "',"
					+ RunConfig.CRAWL_DOMAIN_SEQ
					+ " );";
			
			query = addToQueryListOrExecute (query, singleSentInsert);
			
			if (page.getSentenceAdjMap().containsKey(pos))
				for ( String adj : page.getSentenceAdjMap().get(pos) )
					query = addToQueryListOrExecute (query, genereateSentenceFeatureInsert(sentId, "A", adj));
			
			if (page.getSentenceNounMap().containsKey(pos))
				for ( String noun : page.getSentenceNounMap().get(pos) )				
					query = addToQueryListOrExecute (query, genereateSentenceFeatureInsert(sentId, "N", noun));

			
			if (page.getSentenceEntityMap().containsKey(pos))
				for ( String entity : page.getSentenceEntityMap().get(pos) )
					query = addToQueryListOrExecute (query, genereateSentenceFeatureInsert(sentId, "E", entity));
			
		}
		
		postgres.execUpdate(query);
		currentRequestSize = 0;
		query = "";

	}
	
	public void writeEntities(WebPage page) {

		logger.info("Writing " + page.getEntityMentionMap().keySet().size()
				+ " entities. ");
		
		List<String> entities = new ArrayList(page.getEntityMentionMap().keySet());
			
		for (int i = 0 ; i < entities.size() ; i++){
				
				String entityURI = entities.get(i);
				
				for ( String mention : page.getEntityMentionMap().get(entityURI) ){
				
					String singleEntityMentionInsert = "INSERT INTO ENTITY_MENTIONS ( "
							+ "URI, "
							+ "MENTION_SENT, "
							+ "SITE_ID "
							+ ") "
							+ "VALUES ( "
							+ "'"+ StringUtils.replace(entityURI, "'", "''") + "',"
							+ "'"+ mention + "',"
							+ "'"+ SeqGenerator.getSiteId() + "'"
							+ " );";
					
					query = addToQueryListOrExecute (query, singleEntityMentionInsert);
				
			}
			
		}
		
		
		postgres.execUpdate(query);
		currentRequestSize = 0;
		query = "";

	}
	
	private String genereateSentenceFeatureInsert(String sentId, String key, String value){
		
		return "INSERT INTO SENTENCE_FEATURES ( "
				+ "SENT_ID, "
				+ "KEY, "
				+ "VALUE, "
				+ "SITE_ID "
				+ ") "
				+ "VALUES ( "
				+ "'" + sentId + "',"
				+ "'" + key + "',"
				+ "'" + StringUtils.replace(value, "'", "''") + "',"
				+ "'"+ SeqGenerator.getSiteId() + "'"
				+ " );";
		
	}
	
	private String addToQueryListOrExecute (String existingQuery , String newRequest) {
		
		String newQuery = existingQuery + " " + newRequest;
		
		if (currentRequestSize < maximumRequestSize - 1){
			
			currentRequestSize++;
			return newQuery;
			
		}else{
			
			postgres.execUpdate(newQuery);
			currentRequestSize = 0;
			return "";		
			
		}
		
	}

}
