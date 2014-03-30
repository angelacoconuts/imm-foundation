package com.enhype.db.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.enhype.db.DynamoDB;
import com.enhype.utils.RunConfig;

public class DynamoDBBatchTest {

	static DynamoDB dynamo;
	String tableName = "Sentence";
	
	@BeforeClass 
	public static void initDynamoDB(){
		
		RunConfig.parseCfgFromFile("config.json");		
		dynamo = new DynamoDB();
		try {
			DynamoDB.init();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Test
	public void testCreateTable() {

		String tableKey = "Id";
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put("Id", "S");
		
		dynamo.createTable(tableName, tableKey, tableAttrs, 3, 2);
		
	}
	
	@Test
	public void testAddItemBatch() {

		Map<Integer, Map<String, String>> strAttrsList = new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, List<String>>> strSetAttrsList = new HashMap<Integer, Map<String, List<String>>>();

		Map<String, String> stringAttrs = new HashMap<String, String>();
		Map<String, List<String>> stringSetAttrs = new HashMap<String, List<String>>();
		
		String[] nouns = { "cat" , "dog" };
		String[] entities = { "http://dbpedia.org/page/New_York" , "http://dbpedia.org/page/Chicago" };
		String[] adjs = { "fun" };
				
		stringAttrs.put( "Id", "http://wikitravel.org/en/Hong_Kong_in_a_day||0" );
		stringAttrs.put( "text", "This is a good day." );
		stringSetAttrs.put( "entities", Arrays.asList(entities) );
		stringSetAttrs.put( "nouns", Arrays.asList(nouns) );
		stringSetAttrs.put( "adjs" , Arrays.asList(adjs) );
		
		strAttrsList.put(0, stringAttrs);
		strSetAttrsList.put(0, stringSetAttrs);
		
		stringAttrs = new HashMap<String, String>();
		stringSetAttrs = new HashMap<String, List<String>>();
		
		String[] nouns2 = { "man" , "woman" };
		String[] entities2 = { "http://dbpedia.org/page/Fashion" , "http://dbpedia.org/page/Investment" };
		String[] adjs2 = { "different" };
				
		stringAttrs.put( "Id", "http://wikitravel.org/en/Hong_Kong_in_a_day||1368" );
		stringAttrs.put( "text", "This is the second sentence" );
		stringSetAttrs.put( "entities", Arrays.asList(entities2) );
		stringSetAttrs.put( "nouns", Arrays.asList(nouns2) );
		stringSetAttrs.put( "adjs" , Arrays.asList(adjs2) );
		
		strAttrsList.put(1368, stringAttrs);
		strSetAttrsList.put(1368, stringSetAttrs);

		dynamo.addBatchItems(tableName, strAttrsList, strSetAttrsList);
		
	}

}
