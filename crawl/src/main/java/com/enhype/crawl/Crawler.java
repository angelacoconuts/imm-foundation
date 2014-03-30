package com.enhype.crawl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.enhype.utils.RunConfig;

public class Crawler {
	
	static Set<String> knownEntities = new HashSet<String>();
	static Map<String, String> entitiesList = new HashMap<String, String>();
	static Map<String, Set<String>> entitiesSurfaceList = new HashMap<String, Set<String>>();
	static DBWriter dynamoDB;
	
	public static void main( String[] args )
    {
    	
		if(args.length == 0)
			System.out.println("Usage: java -jar crawler.jar config.json");
			
		RunConfig.parseCfgFromFile(args[0]);
		
		dynamoDB = new DBWriter();		
		dynamoDB.createEntityTable();
		dynamoDB.createPageTable();
		dynamoDB.createSentenceTable();
		
    	crawlPages();
				
    }

	public static void crawlPages() {
		
		CrawlControler crawler = new CrawlControler();

		for (String seedPage : RunConfig.seedPages)
			crawler.crawl(seedPage);

	}
	
}
