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

public class DynamoDBTest {
	
	static DynamoDB dynamo;
	String tableName = "Entity";
	
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

		String tableKey = "url";
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put("url", "S");
		
		dynamo.createTable(tableName, tableKey, tableAttrs, 4, 2);
		
	}
	
	@Test
	public void testAddItem() {

		String[] types = { "City" , "Financial Center" };
		String[] mentions = { "1#3498" , "2#3875" , "3#462356" };
		String[] labels = { "Hong Kong" };
		
		Map<String, String> stringAttrs = new HashMap<String, String>();
		Map<String, List<String>> stringSetAttrs = new HashMap<String, List<String>>();
		
		stringAttrs.put( "url", "http://dbpedia.org/page/Hong_Kong" );
		stringSetAttrs.put( "label", Arrays.asList(labels) );
		stringSetAttrs.put( "types", Arrays.asList(types) );
		stringSetAttrs.put( "mentions" , Arrays.asList(mentions) );
		
		dynamo.addItem(tableName, stringAttrs, stringSetAttrs);
		
	}
	
	@Test
	public void testGetItem() {
		
		boolean exist = dynamo.doesItemExist(tableName, "url", "http://dbpedia.org/page/Hong_Kong");
		assertTrue(exist);
		
		exist = dynamo.doesItemExist(tableName, "url", "http://dbpedia.org/page/Hong");
		assertFalse(exist);
		
	}
	
	@Test
	public void testAddValue() {	
		
		String[] mentions = { "4||2462" , "5||2346" , "6||4646" };		
		Map<String, List<String>> strSetAttrUpdates = new HashMap<String, List<String>>();
		strSetAttrUpdates.put( "mentions" , Arrays.asList(mentions) );
		
		dynamo.addNewValueToItem(tableName, "url", "http://dbpedia.org/page/Hong_Kong", strSetAttrUpdates);	
		
	}

}
