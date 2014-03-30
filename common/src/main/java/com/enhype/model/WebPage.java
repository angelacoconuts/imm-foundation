package com.enhype.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WebPage {
	
	private String id;
	private String URL;
	private String title;
	private String keywords; 
	private String fetchTime;	
	private Map<Integer, String> sentencePositionMap;
	private Map<Integer, Set<String>> sentenceAdjMap;
	private Map<Integer, Set<String>> sentenceNounMap;
	private Map<Integer, Set<String>> sentenceEntityMap;
	private Map<String, Set<String>> entityMentionMap;
	
	public String getURL() {
		return URL;
	}
	public void setURL(String uRL) {
		URL = uRL;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getFetchTime() {
		return fetchTime;
	}
	public void setFetchTime(String fetchTime) {
		this.fetchTime = fetchTime;
	}
	public String getKeywords() {
		return keywords;
	}
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
	public Map<Integer, String> getSentencePositionMap() {
		return sentencePositionMap;
	}
	public void setSentencePositionMap(Map<Integer, String> sentencePositionMap) {
		this.sentencePositionMap = sentencePositionMap;
	}
	public Map<String, Set<String>> getEntityMentionMap() {
		return entityMentionMap;
	}
	public void setEntityMentionMap(Map<String, Set<String>> entityMentionMap) {
		this.entityMentionMap = entityMentionMap;
	}
	public Map<Integer, Set<String>> getSentenceAdjMap() {
		return sentenceAdjMap;
	}
	public void setSentenceAdjMap(Map<Integer, Set<String>> sentenceAdjMap) {
		this.sentenceAdjMap = sentenceAdjMap;
	}
	public Map<Integer, Set<String>> getSentenceNounMap() {
		return sentenceNounMap;
	}
	public void setSentenceNounMap(Map<Integer, Set<String>> sentenceNounMap) {
		this.sentenceNounMap = sentenceNounMap;
	}
	public Map<Integer, Set<String>> getSentenceEntityMap() {
		return sentenceEntityMap;
	}
	public void setSentenceEntityMap(Map<Integer, Set<String>> sentenceEntityMap) {
		this.sentenceEntityMap = sentenceEntityMap;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
}
