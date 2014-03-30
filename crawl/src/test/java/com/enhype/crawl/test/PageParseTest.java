package com.enhype.crawl.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.enhype.crawl.PageParser;
import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

public class PageParseTest {

	static PageParser parser = new PageParser();
	static File htmlFile = new File("src/test/resources/Hong Kong in a day - Wikitravel.html");
	static String url = "http://wikitravel.org/en/Hong_Kong_in_a_day";
	static WebPage page;
	
	@BeforeClass 
	public static void initNLPUtils(){
		
		RunConfig.parseCfgFromFile("config.json");
		page = parser.parse(url, htmlFile);
		
	}
	
	@Test
	public void testPageParseBasic() {
					
		assertTrue( page.getURL() == "http://wikitravel.org/en/Hong_Kong_in_a_day" );
		assertTrue( page.getTitle().startsWith( "Hong Kong in a day - Wikitravel" ) );
		assertTrue( page.getKeywords().contains("wikitravel") );
		assertTrue( page.getKeywords().contains("Hong Kong") );
		
	}
	
	@Test
	public void testPageEntities() {
		
		Map<Integer, Set<String>> entities = page.getSentenceEntityMap();					
		assertTrue( entities.get(0).contains("http://dbpedia.org/resource/Hong_Kong") );
		
	}

}
