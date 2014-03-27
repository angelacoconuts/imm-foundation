package org.model;

import java.util.List;
import java.util.Map;

public class WebPage {
	
	private String URL;
	private String title;
	private String keywords; 
	private String fetchTime;	
	private Map<Integer, String> sentencePositionMap;
	private Map<Integer, List<String>> sentenceAdjAdvMap;
	private Map<Integer, List<String>> sentenceEntityMap;
	
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
	public Map<Integer, List<String>> getSentenceAdjAdvMap() {
		return sentenceAdjAdvMap;
	}
	public void setSentenceAdjAdvMap(Map<Integer, List<String>> sentenceAdjAdvMap) {
		this.sentenceAdjAdvMap = sentenceAdjAdvMap;
	}
	public Map<Integer, List<String>> getSentenceEntityMap() {
		return sentenceEntityMap;
	}
	public void setSentenceEntityMap(Map<Integer, List<String>> sentenceEntityMap) {
		this.sentenceEntityMap = sentenceEntityMap;
	}
	
}
