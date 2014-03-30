package com.enhype.db;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnection {
	
	private Logger logger = Logger.getLogger(CassandraConnection.class.getName());
	private Session session;
	
	public CassandraConnection() {
		
		try{
			
			Cluster cluster = Cluster.builder()
			  .addContactPoints(CassandraConfig.serverIP)
			  .build();
		
			session = cluster.connect(CassandraConfig.keySpace);
			
		}catch (Exception e) {

			logger.error("Cassandra connection exception", e);
		}
		
	}
	
	public boolean testConnection() {
		
		for (Row row : session.execute(CassandraConfig.CQL_TEST_CONNECTION)) {
			logger.info(row.toString());
		}
		
		return true;
		
	}
	
}
