package org.crawl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.model.WebPage;
import org.utils.RunConfig;

public class PageParser {

	private Logger logger = Logger.getLogger(PageParser.class.getName());

	private static String CONTENT_TAGS = "p, br, table, h1, h2, h3, h4, h5, h6";
	private NLPUtils nlpTlk = new NLPUtils(); 
	private WebPage page = new WebPage();
	private String text = "";

	public WebPage parse(String url, String html){
		
		String rawText = "";
		String[] sentences = null;
		Document doc = null;
		int sentencePosPointer = 0;
		int[] sentencePositions;
		Map<Integer, String> sentencePositionMap = new LinkedHashMap<Integer, String>();
		Map<Integer, List<String>> sentenceAdjMap = new HashMap<Integer, List<String>>();
		Map<Integer, List<String>> sentenceNounMap = new HashMap<Integer, List<String>>();
		Map<Integer, List<String>> sentenceEntityMap = new HashMap<Integer, List<String>>();
		
		long timer = System.currentTimeMillis();
		
		doc = Jsoup.parse(html, "UTF-8");
		
		long jsoupTime = System.currentTimeMillis() - timer;
		logger.info( "Jsoup parse time: " + ( jsoupTime ) + " ms");
		
		rawText = doc.select(CONTENT_TAGS).text();			
		sentences = nlpTlk.splitSentences(rawText);
		
		logger.info( "Split sentence time: " + ( System.currentTimeMillis() - timer - jsoupTime ) + " ms");
		
		page.setURL(url);
		page.setFetchTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
				.format(new Timestamp((new java.util.Date()).getTime())));
		page.setTitle(doc.select("title").text());
		page.setKeywords(doc.select("meta[name=keywords]").attr("content"));
		
		sentencePositions = new int[sentences.length + 1];				
			
		parseAdjAdvs(sentences, sentencePosPointer, sentencePositions, sentenceAdjMap, sentenceNounMap, sentencePositionMap);
		
		parseEntities(sentencePositions, sentenceEntityMap);
		
		page.setSentencePositionMap(sentencePositionMap);
		page.setSentenceAdjMap(sentenceAdjMap);
		page.setSentenceNounMap(sentenceNounMap);
		page.setSentenceEntityMap(sentenceEntityMap);
		
		logger.info( "Parse page total time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
		logParseResult();

		return page;

	}
	
	private void parseAdjAdvs(String[] sentences, int sentencePosPointer, int[] sentencePositions, Map<Integer, List<String>> sentenceAdjMap, Map<Integer, List<String>> sentenceNounMap, Map<Integer, String> sentencePositionMap){
		
		long timer = System.currentTimeMillis();
		
		//Mark the start of every sentence, title starts at position 0
		sentencePositions[0] = sentencePosPointer;		
		text += page.getTitle() + " " + page.getKeywords();
		sentencePositionMap.put(sentencePosPointer, text);
		sentencePosPointer += text.length();
		
		for (int i = 0 ; i < sentences.length ; i++){

			sentencePositionMap.put(sentencePosPointer, sentences[i]);
			
			List<String> adjList = new ArrayList<String>();
			List<String> nounList = new ArrayList<String>();
			
			nlpTlk.getAdjNounList(sentences[i], adjList, nounList);	
			
			if (adjList.size() > 0){
				sentenceAdjMap.put(sentencePosPointer, adjList);
			}
			if (nounList.size() > 0){
				sentenceNounMap.put(sentencePosPointer, nounList);
			}
			
			sentencePositions[i + 1] = sentencePosPointer;
			text += sentences[i] + " ";			
			sentencePosPointer += sentences[i].length() + 1;
							
		}
		
		logger.info( "Tag adj/adv time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
	}
	
	private void parseEntities(int[] sentencePositions, Map<Integer, List<String>> sentenceEntityMap){
		
		long timer = System.currentTimeMillis();
		Map<Integer, String> entityPositionMap = new HashMap<Integer, String>();
		String[] chunks = text.split("(?<=\\G.{"+RunConfig.URL_LEN_LIMIT+"})");
				
		for ( int i = 0 ; i < chunks.length ; i++ )
			entityPositionMap.putAll( nlpTlk.getEntityList(chunks[i], i) );
		
		for ( int pos : entityPositionMap.keySet() ){

			int sentSeq = Arrays.binarySearch(sentencePositions, pos);
			String entity = entityPositionMap.get(pos);
			int key = sentencePositions[ sentSeq>=0 ? sentSeq : Math.abs(sentSeq)-2 ];			 
			if(sentenceEntityMap.containsKey(key))
				sentenceEntityMap.get(key).add(entity);
			else{
				List<String> entityList = new ArrayList<String>();
				entityList.add(entity);
				sentenceEntityMap.put(key, entityList);
			}
		}
		
		logger.info( "Tag entities time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
	}
	
	private void logParseResult(){
		
		logger.debug("URL: " + page.getURL());
		logger.info("TITLE: " + page.getTitle());
		logger.debug("FETCHED: " + page.getFetchTime());
		logger.info("KEYWORDS: " + page.getKeywords());
		logger.debug("");
		
		Map<Integer, String> sentencePositionMap = page.getSentencePositionMap();
		Map<Integer, List<String>> sentenceAdjMap = page.getSentenceAdjMap();
		Map<Integer, List<String>> sentenceNounMap = page.getSentenceNounMap();
		Map<Integer, List<String>> sentenceEntityMap = page.getSentenceEntityMap();
		
		for (int key : sentencePositionMap.keySet() ){
			logger.debug("Sent: " + sentencePositionMap.get(key));
			if(sentenceAdjMap.containsKey(key))
				logger.info("Adj: " + sentenceAdjMap.get(key).toString());
			if(sentenceNounMap.containsKey(key))
				logger.info("Noun: " + sentenceNounMap.get(key).toString());
			if(sentenceEntityMap.containsKey(key))
				logger.info("Entity: " + sentenceEntityMap.get(key).toString());
			logger.info("");
		}
		
	}



}
