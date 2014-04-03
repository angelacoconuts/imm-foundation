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

import com.enhype.crawl.io.DynamoDBWriter;
import com.enhype.crawl.io.PostgresDBWriter;
import com.enhype.utils.RunConfig;

public class Crawler {
	
	private static Logger logger = Logger.getLogger(Crawler.class.getName());
//	static DynamoDBWriter dbWriter;
	static PostgresDBWriter dbWriter;
	private static Set<String> knownEntities = new HashSet<String>();
	private static Map<String, String> entitiesList = new HashMap<String, String>();
	private static Map<String, Set<String>> entitiesSurfaceList = new HashMap<String, Set<String>>();	
	private static long pageSeq = RunConfig.PAGE_SEQ;
	private static long lastPageSeq = pageSeq;
	private static long sentSeq;
	private static int siteSeq;
	
	static SortedSet<String> seedURLs = new TreeSet<String>();
	
	public static void main( String[] args )
    {
    	
		if(args.length == 0){
			logger.error("Usage: java -jar crawler.jar config.json");
			return;
		}
			
		RunConfig.parseCfgFromFile(args[0]);
		
		dbWriter = new PostgresDBWriter();
		
		if(args.length > 1 && args[1] == "createtable"){

			dbWriter.createEntityTable();
			dbWriter.createPageTable();
			dbWriter.createSiteTable();
			dbWriter.createSentenceTable();
			
		}else if (args.length > 1 && args[1] == "reloadtable") {
			
			dbWriter.dropAllTables();
			dbWriter.createEntityTable();
			dbWriter.createPageTable();
			dbWriter.createSiteTable();
			dbWriter.createSentenceTable();
			
		}
		
		if(RunConfig.IS_META_CRAWL == 0){

			dbWriter = new PostgresDBWriter();

			crawlPages();
			
		}
		   	
		else if(RunConfig.IS_META_CRAWL == 1){
			
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

	private static void crawlPages() {
		
		CrawlControler crawler = new CrawlControler();
		
		siteSeq = 1 ; 

		for (String seedPage : RunConfig.seedPages){
			
			sentSeq = 0;
			
			logger.info("Start crawling: " + seedPage);
			crawler.crawl(seedPage);
			
			dbWriter.writeSite(seedPage, lastPageSeq, pageSeq, sentSeq);
			lastPageSeq = pageSeq;
			siteSeq++;
			
		}

	}

	public static Set<String> getKnownEntities() {
		return knownEntities;
	}

	public static Map<String, String> getEntitiesList() {
		return entitiesList;
	}

	public static Map<String, Set<String>> getEntitiesSurfaceList() {
		return entitiesSurfaceList;
	}

	public static int getSiteSeq() {
		return siteSeq;
	}
	
	public static long getPageSeq() {
		return pageSeq;
	}
	
	public static long getSentSeq() {
		return sentSeq;
	}
	
	public static void setPageSeq(long pageSeq) {
		Crawler.pageSeq = pageSeq;
	}
	
	public static void setSentSeq(long sentSeq) {
		Crawler.sentSeq = sentSeq;
	}
}
