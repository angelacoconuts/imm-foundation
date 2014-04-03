package com.enhype.crawl;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.enhype.crawl.io.SeqGenerator;
import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

public class PageParser {

	private Logger logger = Logger.getLogger(PageParser.class.getName());

	private static String CONTENT_TAGS = "p, br, table, h1, h2, h3, h4, h5, h6";
	private NLPUtils nlpTlk = new NLPUtils(); 
	private WebPage page = new WebPage();
	private String text = "";

	public void parseURL(File file){
		
		Document doc = null;	
		try {
			doc = Jsoup.parse(file, "UTF-8");
		} catch (IOException e) {
			logger.error(e);
		}
		
		for ( Element link : doc.select("a[href]") ){
			String linkStr = link.attr("href");			
			if (linkStr != null && linkStr.length() > 0 && linkStr.startsWith("http") 
					&& !linkStr.contains("yahoo") && !linkStr.contains("google") 
					&& !linkStr.contains("youtube") ){
				Crawler.seedURLs.add(linkStr);
			}
		}

	}
	
	
	public WebPage parse(String url, String html){
		
		Document doc = null;		
		doc = Jsoup.parse(html, "UTF-8");

		return parseJSDoc(url, doc);

	}
	
	public WebPage parse(String url, File htmlFile){
		
		Document doc = null;		
		try {
			doc = Jsoup.parse(htmlFile, "UTF-8");
		} catch (IOException e) {
			logger.error( "Jsoup cannot parse html file specified" , e );
		}

		return parseJSDoc(url, doc);

	}
	
	private WebPage parseJSDoc(String url, Document doc){
		
		String rawText = "";
		String[] sentences = null;
		int sentencePosPointer = 0;
		int[] sentencePositions;
		Map<Integer, String> sentencePositionMap = new LinkedHashMap<Integer, String>();
		Map<Integer, Set<String>> sentenceAdjMap = new HashMap<Integer, Set<String>>();
		Map<Integer, Set<String>> sentenceNounMap = new HashMap<Integer, Set<String>>();
		Map<Integer, Set<String>> sentenceEntityMap = new HashMap<Integer, Set<String>>();
		Map<String, Set<String>> entityMentionMap = new HashMap<String, Set<String>>();
		
		long timer = System.currentTimeMillis();
		
		long jsoupTime = System.currentTimeMillis() - timer;
		logger.debug( "Jsoup parse time: " + ( jsoupTime ) + " ms");
		
		rawText = doc.select(CONTENT_TAGS).text();			
		sentences = nlpTlk.splitSentences(rawText);
		
		logger.debug( "Split sentence time: " + ( System.currentTimeMillis() - timer - jsoupTime ) + " ms");
		
		String pageId = SeqGenerator.generatePageId();
				
		page.setId(pageId);
		
		page.setURL(url);
		page.setFetchTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
				.format(new Timestamp((new java.util.Date()).getTime())));
		page.setTitle(doc.select("title").text());
		page.setKeywords(doc.select("meta[name=keywords]").attr("content"));
		
		sentencePositions = new int[sentences.length + 1];				
			
		parseAdjAdvs(sentences, sentencePosPointer, sentencePositions, sentenceAdjMap, sentenceNounMap, sentencePositionMap);
		
		parseEntities(sentencePositions, sentenceEntityMap, entityMentionMap);
		
		page.setSentencePositionMap(sentencePositionMap);
		page.setSentenceAdjMap(sentenceAdjMap);
		page.setSentenceNounMap(sentenceNounMap);
		page.setSentenceEntityMap(sentenceEntityMap);
		page.setEntityMentionMap(entityMentionMap);
		Crawler.setSentSeq( Crawler.getSentSeq() + sentencePositionMap.keySet().size() );
		
		logger.debug( "Parse page total time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
		logParseResult();

		return page;

	}
	
	private void parseAdjAdvs(String[] sentences, int sentencePosPointer, int[] sentencePositions, Map<Integer, Set<String>> sentenceAdjMap, Map<Integer, Set<String>> sentenceNounMap, Map<Integer, String> sentencePositionMap){
		
		long timer = System.currentTimeMillis();
		
		//Mark the start of every sentence, title starts at position 0
		sentencePositions[0] = sentencePosPointer;		
		text += page.getTitle() + " " + page.getKeywords();
		sentencePositionMap.put(sentencePosPointer, text);
		sentencePosPointer += text.length();
		
		for (int i = 0 ; i < sentences.length ; i++){

			sentencePositionMap.put(sentencePosPointer, sentences[i]);
			
			Set<String> adjList = new HashSet<String>();
			Set<String> nounList = new HashSet<String>();
			
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
		
		logger.debug( "Tag adj/adv time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
	}
	
	private void parseEntities(int[] sentencePositions, Map<Integer, Set<String>> sentenceEntityMap, Map<String, Set<String>> entityMentionMap){
		
		long timer = System.currentTimeMillis();
		Map<Integer, String> entityPositionMap = new HashMap<Integer, String>();
		String[] chunks = text.split("(?<=\\G.{"+RunConfig.URL_LEN_LIMIT+"})");
		Set<String> sentenceIds = new HashSet<String>();
				
		for ( int i = 0 ; i < chunks.length ; i++ ){
			
			if(chunks[i].length() <= 1)
				continue;
			
			entityPositionMap.putAll( nlpTlk.getEntityList(chunks[i], i) );
		}
		
		for ( int pos : entityPositionMap.keySet() ){

			int sentSeq = Arrays.binarySearch(sentencePositions, pos);
			String entity = entityPositionMap.get(pos);
			int key = sentencePositions[ sentSeq>=0 ? sentSeq : Math.abs(sentSeq)-2 ];
			
			if(sentenceEntityMap.containsKey(key))
				sentenceEntityMap.get(key).add(entity);
			else{
				Set<String> entityList = new HashSet<String>();
				entityList.add(entity);
				sentenceEntityMap.put(key, entityList);
			}
			
			String sentenceId = SeqGenerator.getSentId(page.getId(), key);
			sentenceIds.add(sentenceId);
			
			if(entityMentionMap.containsKey(entity))
				entityMentionMap.get(entity).add(sentenceId);
			else{
				Set<String> mentionList = new HashSet<String>();
				mentionList.add(sentenceId);
				entityMentionMap.put(entity, mentionList);
			}
		}
		if(sentenceEntityMap.containsKey(0)){
			Set<String> themes = sentenceEntityMap.get(0);
			for ( String theme : themes )
				entityMentionMap.get(theme).addAll(sentenceIds);
		}
		
		logger.debug( "Tag entities time: " + ( System.currentTimeMillis() - timer ) + " ms");
		
	}
	
	private void logParseResult(){
		
		logger.debug("URL: " + page.getURL());
		logger.debug("TITLE: " + page.getTitle());
		logger.debug("FETCHED: " + page.getFetchTime());
		logger.debug("KEYWORDS: " + page.getKeywords());
		logger.debug("");
		
		Map<Integer, String> sentencePositionMap = page.getSentencePositionMap();
		Map<Integer, Set<String>> sentenceAdjMap = page.getSentenceAdjMap();
		Map<Integer, Set<String>> sentenceNounMap = page.getSentenceNounMap();
		Map<Integer, Set<String>> sentenceEntityMap = page.getSentenceEntityMap();
		
		for (int key : sentencePositionMap.keySet() ){
			logger.debug("Sent: " + sentencePositionMap.get(key));
			if(sentenceAdjMap.containsKey(key))
				logger.debug("Adj: " + sentenceAdjMap.get(key).toString());
			if(sentenceNounMap.containsKey(key))
				logger.debug("Noun: " + sentenceNounMap.get(key).toString());
			if(sentenceEntityMap.containsKey(key))
				logger.debug("Entity: " + sentenceEntityMap.get(key).toString());
			logger.debug("");
		}
		
	}



}
