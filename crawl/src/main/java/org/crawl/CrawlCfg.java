package org.crawl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.io.IOUtils;
import org.jboss.netty.util.internal.StringUtil;

public class CrawlCfg {

	static String CRAWL_DATA_DIR = "";
	static int NUM_CRAWLERS = 1;
	static int NUM_PAGES_TO_FETCH = 1;
	static int CRAWL_DEPTH = 1;
	static String[] seedPages = {};
	static String ENTITY_LINKING_ENDPOINT = "";
	static String CONFIDENCE_LEVEL = "";

	// static String[] ADJ_ADV_TAG_LIST = { "JJ", "JJR", "JJS", "RB", "RBR",
	// "RBS" };
	static String[] ADJ_ADV_TAG_LIST = { "JJ", "JJR", "JJS" };

	public static void parseCfgFromFile(String configJSONFileName) {

		String jsonTxt = "";
		
		try {
			
			FileInputStream fis = new FileInputStream(new File(configJSONFileName));
			jsonTxt = IOUtils.toString(fis);
			
		} catch (FileNotFoundException e1) {
			System.out.println("No config file available.");
		} catch (IOException e) {
			System.out.println("Read config file failure.");
		}

		JSONObject json = (JSONObject) JSONSerializer.toJSON(jsonTxt);
		
		CrawlCfg.CRAWL_DATA_DIR = json.getString("crawl_storage_dir");
		CrawlCfg.NUM_CRAWLERS = json.getInt("crawler_number");
		CrawlCfg.NUM_PAGES_TO_FETCH = json.getInt("max_pages_to_fetch");
		CrawlCfg.CRAWL_DEPTH = json.getInt("max_crawl_depth");
		CrawlCfg.seedPages = StringUtil.split(json.getString("seedPages"),',');
		
		CrawlCfg.ENTITY_LINKING_ENDPOINT = json.getString("entity_linking_service_endpoint");
		CrawlCfg.CONFIDENCE_LEVEL = json.getString("entity_linking_confidence_level");

	}

}
