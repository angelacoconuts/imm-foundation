package org.crawl;

import org.db.AmazonDynamoDB;
import org.utils.RunConfig;

public class App {
	public static void main( String[] args )
    {
    //    CassandraConnection cass = new CassandraConnection();
    //    cass.testConnection();
    	
		if(args.length == 0)
			System.out.println("Usage: java -jar crawler.jar config.json");
			
		RunConfig.parseCfgFromFile(args[0]);
		
    	crawlPages();
				
    }

	public static void crawlPages() {
		
		CrawlControler crawler = new CrawlControler();

		for (String seedPage : RunConfig.seedPages)
			crawler.crawl(seedPage);

	}
	
}
