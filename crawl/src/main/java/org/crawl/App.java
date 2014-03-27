package org.crawl;

public class App {
	public static void main( String[] args )
    {
    //    CassandraConnection cass = new CassandraConnection();
    //    cass.testConnection();
    	
		if(args.length == 0)
			System.out.println("Usage: java -jar crawler.jar config.json");
			
    	crawlPages(args[0]);
				
    }

	public static void crawlPages(String configFileName) {
		
		CrawlCfg.parseCfgFromFile(configFileName);

		CrawlControler crawler = new CrawlControler();

		for (String seedPage : CrawlCfg.seedPages)
			crawler.crawl(seedPage);

	}
}
