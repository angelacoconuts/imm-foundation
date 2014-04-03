package com.enhype.db.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.enhype.db.DynamoDB;
import com.enhype.db.PostgresDB;
import com.enhype.utils.RunConfig;

public class PostgresDBTest {
	
	PostgresDB postgres = new PostgresDB();
	
	@BeforeClass 
	public static void initDynamoDB(){
		
		RunConfig.parseCfgFromFile("config.json");
		
	}
	
	@Test
	public void testCreateTable() {
		
		String updateString = "CREATE TABLE SITES ( SITE_ID INT, ENTRY_POINT VARCHAR(255) );";
		
		postgres.execUpdate( updateString );
		
	}

}
