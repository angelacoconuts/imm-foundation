package com.enhype.utils;

import com.enhype.db.PostgresDB;

public class DBBulkInserter {

	private int maximumRequestSize = 100;
	private int currentRequestSize = 0;
	private String query = "";
	private PostgresDB db = new PostgresDB();
		
	public void addToQueryListOrExecute (String newRequest) {
		
		String newQuery = this.query + " " + newRequest;
		
		if (currentRequestSize < maximumRequestSize - 1){
			
			currentRequestSize++;
			this.query = newQuery;
			
		}else{
			
			db.execUpdate(newQuery);
			currentRequestSize = 0;
			this.query = "";
			
		}
		
	}
	
}
