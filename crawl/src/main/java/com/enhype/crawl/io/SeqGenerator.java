package com.enhype.crawl.io;

import com.enhype.crawl.Crawler;
import com.enhype.utils.RunConfig;

public class SeqGenerator {
	
	public static String generatePageId(){
		
		long pageSeq = Crawler.getPageSeq();
		String pageId = RunConfig.MACHINE_ID + RunConfig.DELIMITOR + "P" + Long.toString(pageSeq);
		Crawler.setPageSeq( pageSeq + 1 );
		return pageId;
		
	}
	
	public static String getPageId(long pageSeq){
		return RunConfig.MACHINE_ID + RunConfig.DELIMITOR + "P" + Long.toString(pageSeq);		
	}
	
	public static String getSiteId(){
		return RunConfig.MACHINE_ID + RunConfig.DELIMITOR + "S" + Integer.toString(Crawler.getSiteSeq());
	}
	
	public static String getSentId(String pageId, int sentPos){
		return pageId + RunConfig.DELIMITOR + sentPos;
	}

}
