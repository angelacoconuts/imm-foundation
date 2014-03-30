package com.enhype.crawl.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.enhype.crawl.NLPUtils;

public class NLPUtilsTest
{
	private static NLPUtils nlpTlk;
	
	@BeforeClass 
	public static void initNLPUtils(){
		
		nlpTlk = new NLPUtils();
		
	}
	
	@Test
    public void testNLPUtilsSplit()
    {
    	
    	String text = "If you still have energy left, visit the area around Temple Street in Yau Ma Tei"
    			+ " where there are many hawker stalls to be found in the evening."
    			+ " This is a 15 minute walk from Mong Kok,"
    			+ " or one stop down the Tsuen Wan Line on the MTR from Mong Kok.";
    	String[] sentences;
    	
        sentences = nlpTlk.splitSentences(text);
        assertTrue( sentences.length == 2 );
        
    }
	
	@Test
    public void testNLPUtilsTagAdjNoun()
    {
    	String sentence = "This is a 15 minute walk from Mong Kok, or one stop down the Tsuen Wan Line on the MTR from Mong Kok.";
    	Set<String> adjList = new HashSet<String>();
    	Set<String> nounList = new HashSet<String>();
    	Set<String> result = new HashSet<String>();

    	nlpTlk.getAdjNounList(sentence, adjList, nounList);

    	result.add("minute");
        assertTrue( adjList.containsAll(result) );
        
        result.clear();
    	result.add("walk");
    	result.add("stop");
        assertTrue( nounList.containsAll(result) );
        
    }
	
}
