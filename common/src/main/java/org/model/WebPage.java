package org.model;

import java.util.List;
import java.util.Map;

public class WebPage {
	
	private String URL;
	private String title;
	private String keywords; 
	private String fetchTime;	
	private Map<Integer, String> sentencePositionMap;
	private Map<Integer, List<String>> sentenceAdjMap;
	private Map<Integer, List<String>> sentenceNounMap;
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
	public Map<Integer, List<String>> getSentenceEntityMap() {
		return sentenceEntityMap;
	}
	public void setSentenceEntityMap(Map<Integer, List<String>> sentenceEntityMap) {
		this.sentenceEntityMap = sentenceEntityMap;
	}
	public Map<Integer, List<String>> getSentenceAdjMap() {
		return sentenceAdjMap;
	}
	public void setSentenceAdjMap(Map<Integer, List<String>> sentenceAdjMap) {
		this.sentenceAdjMap = sentenceAdjMap;
	}
	public Map<Integer, List<String>> getSentenceNounMap() {
		return sentenceNounMap;
	}
	public void setSentenceNounMap(Map<Integer, List<String>> sentenceNounMap) {
		this.sentenceNounMap = sentenceNounMap;
	}
	
}
