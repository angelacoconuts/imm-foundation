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
		Map<Integer, List<String>> sentenceAdjAdvMap = new HashMap<Integer, List<String>>();
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
			
		parseAdjAdvs(sentences, sentencePosPointer, sentencePositions, sentenceAdjAdvMap, sentencePositionMap);
		
		parseEntities(sentencePositions, sentenceEntityMap);
		
		page.setSentencePositionMap(sentencePositionMap);
		page.setSentenceAdjAdvMap(sentenceAdjAdvMap);
		page.setSentenceEntityMap(sentenceEntityMap);
		
		logger.info( "Parse page total time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
		logParseResult();

		return page;

	}
	
	private void parseAdjAdvs(String[] sentences, int sentencePosPointer, int[] sentencePositions, Map<Integer, List<String>> sentenceAdjAdvMap, Map<Integer, String> sentencePositionMap){
		
		long timer = System.currentTimeMillis();
		
		//Mark the start of every sentence, title starts at position 0
		sentencePositions[0] = sentencePosPointer;		
		text += page.getTitle() + " " + page.getKeywords();
		sentencePositionMap.put(sentencePosPointer, text);
		sentencePosPointer += text.length();
		
		for (int i = 0 ; i < sentences.length ; i++){

			sentencePositionMap.put(sentencePosPointer, sentences[i]);
			
			List<String> adjAdvList = nlpTlk.getAdjAdvList(sentences[i]);	
			
			if (adjAdvList.size() > 0){
				sentenceAdjAdvMap.put(sentencePosPointer, adjAdvList);
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
		String[] chunks = text.split("(?<=\\G.{"+CrawlCfg.URL_LEN_LIMIT+"})");
				
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
		logger.debug("TITLE: " + page.getTitle());
		logger.debug("FETCHED: " + page.getFetchTime());
		logger.debug("KEYWORDS: " + page.getKeywords());
		logger.debug("");
		
		Map<Integer, String> sentencePositionMap = page.getSentencePositionMap();
		Map<Integer, List<String>> sentenceAdjAdvMap = page.getSentenceAdjAdvMap();
		Map<Integer, List<String>> sentenceEntityMap = page.getSentenceEntityMap();
		
		for (int key : sentencePositionMap.keySet() ){
			logger.info("Sent: " + sentencePositionMap.get(key));
			if(sentenceAdjAdvMap.containsKey(key))
				logger.debug("Adj: " + sentenceAdjAdvMap.get(key).toString());
			if(sentenceEntityMap.containsKey(key))
				logger.info("Entity: " + sentenceEntityMap.get(key).toString());
			logger.debug("");
		}
		
	}



}
