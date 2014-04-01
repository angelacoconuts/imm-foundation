package com.enhype.crawl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class Crawler {
	
	private static Logger logger = Logger.getLogger(Crawler.class.getName());
	static DBWriter dynamoDB;
	static Set<String> knownEntities = new HashSet<String>();
	static Map<String, String> entitiesList = new HashMap<String, String>();
	static Map<String, Set<String>> entitiesSurfaceList = new HashMap<String, Set<String>>();	
	static SortedSet<String> seedURLs = new TreeSet<String>();
	static long pageSeq = RunConfig.PAGE_SEQ;
	static long lastPageSeq = pageSeq;
	
	public static void main( String[] args )
    {
    	
		if(args.length == 0){
			logger.error("Usage: java -jar crawler.jar config.json");
			return;
		}
			
		RunConfig.parseCfgFromFile(args[0]);
		
		if(RunConfig.IS_META_CRAWL == 0){
			
			dynamoDB = new DBWriter();		
			dynamoDB.createEntityTable();
			dynamoDB.createPageTable();
			dynamoDB.createSiteTable();
			dynamoDB.createSentenceTable();
			
			crawlPages();
			
		}
		   	
		if(RunConfig.IS_META_CRAWL == 1){
			
			PageParser parser = new PageParser();
			
			File folder = new File("src/main/resources/travel");
			File[] listOfFiles = folder.listFiles();

			for (File file : listOfFiles) {
				if(file.isFile())
					parser.parseURL(file);
			}

			FileWriter writer;
			try {
				
				writer = new FileWriter("seed_urls.txt");
				for(String site : seedURLs)
					writer.write(site + "\n");
				writer.close();
				
			} catch (IOException e) {
				logger.error(e);
			}
			
		}
				
    }

	public static void crawlPages() {
		
		CrawlControler crawler = new CrawlControler();

		for (String seedPage : RunConfig.seedPages){
			logger.info("Start crawling: " + seedPage);
			crawler.crawl(seedPage);
			dynamoDB.writeSite(seedPage, lastPageSeq, pageSeq);
			lastPageSeq = pageSeq;
		}

	}
	
}
