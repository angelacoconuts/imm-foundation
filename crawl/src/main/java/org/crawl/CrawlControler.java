package org.crawl;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlControler {
	
	private Logger logger = Logger.getLogger(CrawlControler.class.getName());

	private CrawlConfig crawlConfig;
	private int numberOfCrawlers = CrawlCfg.NUM_CRAWLERS;
	private int maxPagesToFetch = CrawlCfg.NUM_PAGES_TO_FETCH;
	private int maxDepthOfCrawling = CrawlCfg.CRAWL_DEPTH;
	private String crawlDataStore = CrawlCfg.CRAWL_DATA_DIR;
	
	public void crawl(String seedPage){
		
		setCrawlConfig();
	
        PageFetcher pageFetcher = new PageFetcher(this.crawlConfig);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller;
        
		try {
			
			controller = new CrawlController(this.crawlConfig, pageFetcher, robotstxtServer);
			
			controller.addSeed(seedPage);
						
			controller.setCustomData(seedPage);
	        
	        /*
	         * Start the crawl. This is a blocking operation, meaning that your code
	         * will reach the line after this only when crawling is finished.
	         */
	        controller.start(CrawlVisitor.class, this.numberOfCrawlers);
	        
		} catch (Exception e) {

			logger.error("Crawler exception", e);
		}
        
	}
	
	public void crawl(String seedPage, int numberOfCrawlers, int maxDepthOfCrawling, int maxPagesToFetch){
		
		this.numberOfCrawlers = numberOfCrawlers;
		this.maxDepthOfCrawling = maxDepthOfCrawling;
		this.maxPagesToFetch = maxPagesToFetch;
		crawl(seedPage);
		
	}
	
	private void setCrawlConfig(){
		
		crawlConfig = new CrawlConfig();
		crawlConfig.setCrawlStorageFolder(this.crawlDataStore);    
		crawlConfig.setMaxDepthOfCrawling(this.maxDepthOfCrawling);
		crawlConfig.setMaxPagesToFetch(this.maxPagesToFetch);
	//	crawlConfig.setResumableCrawling(true);
		
	}

}
