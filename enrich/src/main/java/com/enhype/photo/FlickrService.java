package com.enhype.photo;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.enhype.db.PostgresDB;
import com.enhype.utils.DBBulkInserter;
import com.enhype.utils.RunConfig;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

public class FlickrService {

	private static Logger logger = Logger.getLogger(FlickrService.class.getName());
	
	private static String key = "c1a999f2ec12e47f97fe1736b50cc5cb";
	private static String photoSearchMethod = "flickr.photos.search";
	private static String ccBySANonCom = "4,5,6,7";
	private static String sortBYInterestingness = "interestingness-desc";
	private static String sortBYRelevance = "relevance";
	private PostgresDB db = new PostgresDB();

	public List<String> getCCBySAPhotoFlickr(String query){
		
		List<String> imgSrcs = new ArrayList<String>();

		
		WebResource resource = Client.create()
				.resource(RunConfig.FLICKR_SERVICE_ENDPOINT)
				.path("services/rest/")	
				.queryParam("method", photoSearchMethod)
				.queryParam("api_key", key)
				.queryParam("text", query)
				.queryParam("format", "json")
				.queryParam("nojsoncallback", "1")
				.queryParam("license", ccBySANonCom)
				.queryParam("sort", sortBYRelevance);
		
		ClientResponse response = null;
		logger.info(resource.toString());
		
		try{

			response = resource
					.accept(MediaType.APPLICATION_JSON)
					.get(ClientResponse.class);
			
		}catch (Exception e)
		{ 
			logger.error("Unable to connection to entity linking service" , e);
			return null;
		}

		if (response.getStatus() >= 300) {
			logger.error(String.format(
					"GET [%s], status code [%d]",
					resource.toString(), response.getStatus()));
			throw new UniformInterfaceException(
					"Status code indicates request not expected", response);
		}

		String responseStr = response.getEntity(String.class);
		
		logger.info(responseStr);

		if (response != null)
			response.close();

		JSONArray photos = (JSONArray) ((JSONObject)JSONObject.fromObject(responseStr).get("photos")).get("photo");
		
		for (Object photoObj : photos){
			JSONObject photo = (JSONObject) photoObj ;
			Photo pho = new Photo(photo.getString("id"), photo.getString("owner"), photo.getString("secret"), photo.getString("server"), photo.getString("farm"), photo.getString("title"));
			imgSrcs.add(pho.getUrl());
			
			String updateDBStr = "insert into pictures ( query , img_src , img_owner , img_title ) "
					+ "values ("
					+ "'" + StringUtils.replace(query," ","_") + "', "
					+ "'" + pho.getUrl() + "', "
					+ "'" + pho.getOwner() + "', "
					+ "'" + StringUtils.replace(pho.getTitle(),"'","''") + "' "
					+ ");" ; 
			
			//DBBulkInserter dbInserter = new DBBulkInserter();
			//dbInserter.addToQueryListOrExecute(updateDBStr);
			db.execUpdate(updateDBStr);
			
		}
		
		return imgSrcs;
		
	}
	
}
