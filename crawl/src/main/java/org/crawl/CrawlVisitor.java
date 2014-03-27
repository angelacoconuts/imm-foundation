package org.crawl;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

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
	
		String url = page.getWebURL().getURL();
		logger.info("Fetched page: " + url);

		if (page.getParseData() instanceof HtmlParseData) {
					
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String html = htmlParseData.getHtml();
			
			PageParser parser = new PageParser();
			parser.parse(url, html);
			
		}		

		
	}
}
