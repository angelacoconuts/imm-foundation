package com.enhype.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.io.IOUtils;
import org.jboss.netty.util.internal.StringUtil;

public class RunConfig {

	public static String CRAWL_DATA_DIR = "";
	public static String AWSCredentialDir = "";
	public static String MACHINE_ID = "";
	public static long PAGE_SEQ = 1;
	public static int NUM_CRAWLERS = 1;
	public static int NUM_PAGES_TO_FETCH = 1;
	public static int CRAWL_DEPTH = 1;
	public static String[] seedPages = {};
	public static String ENTITY_LINKING_ENDPOINT = "";
	public static String CONFIDENCE_LEVEL = "";
	public static long ENTITY_READ_CAPACITY = 10;
	public static long ENTITY_WRITE_CAPACITY = 5;
	public static long SENT_READ_CAPACITY = 10;
	public static long SENT_WRITE_CAPACITY = 5;

	// static String[] ADJ_ADV_TAG_LIST = { "JJ", "JJR", "JJS", "RB", "RBR",
	// "RBS" };
	public static String[] ADJ_ADV_TAG_LIST = { "JJ", "JJR", "JJS" };
	public static String[] NOUN_TAG_LIST = { "NN", "NNS" };
	public static int URL_LEN_LIMIT = 7000;

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
		
		RunConfig.CRAWL_DATA_DIR = json.getString("crawl_storage_dir");
		RunConfig.NUM_CRAWLERS = json.getInt("crawler_number");
		RunConfig.NUM_PAGES_TO_FETCH = json.getInt("max_pages_to_fetch");
		RunConfig.CRAWL_DEPTH = json.getInt("max_crawl_depth");
		RunConfig.seedPages = StringUtil.split(json.getString("seedPages"),',');
		RunConfig.AWSCredentialDir = json.getString("aws_credential_dir");
		RunConfig.MACHINE_ID = json.getString("machine_id");
		RunConfig.PAGE_SEQ = json.getLong("page_seq");
		RunConfig.ENTITY_READ_CAPACITY = json.getLong("entity_read_capacity");
		RunConfig.ENTITY_WRITE_CAPACITY = json.getLong("entity_write_capacity");
		RunConfig.SENT_READ_CAPACITY = json.getLong("sent_read_capacity");
		RunConfig.SENT_WRITE_CAPACITY = json.getLong("sent_write_capacity");
		
		RunConfig.ENTITY_LINKING_ENDPOINT = json.getString("entity_linking_service_endpoint");
		RunConfig.CONFIDENCE_LEVEL = json.getString("entity_linking_confidence_level");

	}

}
