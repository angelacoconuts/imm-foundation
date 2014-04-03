package com.enhype.crawl;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class CrawlVisitor extends WebCrawler {
	
	private Logger logger = Logger.getLogger(CrawlVisitor.class.getName());
	
	private final static Pattern IGNORE_MEDIA = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	
	private final static Pattern IGNORE_SUBPAGE = Pattern.compile(".*\\?.*=.*");

	/**
	 * Specify whether the given url should be crawled or not.
	 */
	@Override
	public boolean shouldVisit(WebURL url) {
		
	    String seedDomain = (String) this.getMyController().getCustomData();
	    
	    String href = url.getURL().toLowerCase();
	    
	    if (IGNORE_SUBPAGE.matcher(href).matches() || IGNORE_MEDIA.matcher(href).matches()) {
	       return false;
	    }

	    if (href.startsWith(seedDomain)) {
	       return true;
	    }
	    
	    return false;
	    
	}

	/**
	 * This function is called when a page is fetched and ready to be processed
	 */
	@Override
	public void visit(Page page) {
		
		long start = System.currentTimeMillis();
	
		String url = page.getWebURL().getURL();
		logger.info("Fetched page: " + url);

		if (page.getParseData() instanceof HtmlParseData) {
					
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String html = htmlParseData.getHtml();
			
			PageParser parser = new PageParser();
				
				//Crawl page and parse entities and adjectives
				WebPage parsedPage = parser.parse(url, html);
				
				long timer = System.currentTimeMillis();			
				Crawler.dbWriter.writePage(parsedPage);
				long delta_timer = System.currentTimeMillis() - timer;
				logger.info("Write page time: " + delta_timer + "ms"); 
				
				Crawler.dbWriter.writeSentences(parsedPage);
				logger.info("Write sentence time: " + (System.currentTimeMillis() - timer - delta_timer) + "ms"); 
				delta_timer = System.currentTimeMillis() - timer;
				
				Crawler.dbWriter.writeEntities(parsedPage);
				logger.info("Write entity time: " + (System.currentTimeMillis() - timer - delta_timer) + "ms"); 
			
		}
		
		logger.info("=== Process page total time: " + (System.currentTimeMillis() - start) + "ms ==="); 
		
	}
}
