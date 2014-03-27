package org.crawl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class NLPUtils {
	
	private Logger logger = Logger.getLogger(PageParser.class.getName());
	
	SentenceDetectorME sentenceDetector = null;
	Tokenizer tokenizer = null;
	POSTaggerME tagger = null;
	
	public NLPUtils(){
		
		InputStream senModelIn = null;
		InputStream tokModelIn = null;
		InputStream posModelIn = null;
		
		SentenceModel senModel = null;
		TokenizerModel tokModel = null;
		POSModel posModel = null;	
		
		try {			
			senModelIn = new FileInputStream("src/main/resources/en-sent.bin");
			senModel = new SentenceModel(senModelIn);			
			
			tokModelIn = new FileInputStream("src/main/resources/en-token.bin");
			tokModel = new TokenizerModel(tokModelIn);	
			
			posModelIn = new FileInputStream("src/main/resources/en-pos-maxent.bin");
			posModel = new POSModel(posModelIn);
			
		} 
		catch (FileNotFoundException e) {
			logger.error("OpenNLP model file not found", e);
		}
		catch (IOException e) {
			logger.error("Load openNLP model error", e);
		} finally {
			if (senModelIn != null) {
				try {
					senModelIn.close();
				} catch (IOException e) {
					logger.error("Load openNLP model error", e);
				}
			}
			if (tokModelIn != null) {
				try {
					tokModelIn.close();
				} catch (IOException e) {
					logger.error("Load openNLP model error", e);
				}
			}
			if (posModelIn != null) {
				try {
					posModelIn.close();
				} catch (IOException e) {
					logger.error("Load openNLP model error", e);
				}
			}
		}
		
		if (senModel != null){
			sentenceDetector = new SentenceDetectorME(senModel);
		}
		if (tokModel != null){			
			tokenizer = new TokenizerME(tokModel);
		}
		if (posModel != null){
			tagger = new POSTaggerME(posModel);					
		}
		
	}
	
	public List<String> getAdjAdvList(String sentence) {
		
		List<String> adjAdvList = new ArrayList<String>();
		String[] tokens = tokenize(sentence);
		String[] tags = tagPOS(tokens);
		
		for (int i = 0; i < tags.length ; i++)
			if ( Arrays.binarySearch(CrawlCfg.ADJ_ADV_TAG_LIST, tags[i]) >= 0 )
				adjAdvList.add(tokens[i]);
		
		return adjAdvList;
		
	}
	
	public Map<Integer, String> getEntityList(String rawText) {
		
		Map<Integer, String> entityList = new HashMap<Integer, String>();
		
		//Call entity linking service;
		JSONObject linkedResult = callEntityLinkingAnnotate(
				CrawlCfg.ENTITY_LINKING_ENDPOINT, 
				rawText, 
				CrawlCfg.CONFIDENCE_LEVEL);
		
		if( linkedResult == null )
			return entityList;
		
		JSONArray resources = linkedResult.getJSONArray("Resources");
		
		for ( int i = 0; i < resources.size(); i++ ){
			JSONObject resource  = resources.getJSONObject(i);
			entityList.put(Integer.valueOf(resource.getString("@offset")), resource.getString("@URI") + "#" + resource.getString("@types") );
		}		
			
		return entityList;
		
	}
	
	private JSONObject callEntityLinkingAnnotate(String serviceEndpoint, String textToAnnotate, String confidence) {

		if( textToAnnotate == null || textToAnnotate.length() == 0 )
			return null;
		
		WebResource resource = Client.create()
				.resource(serviceEndpoint)
				.path("annotate")
				.queryParam("text", textToAnnotate)
				.queryParam("confidence", confidence);
		ClientResponse response = null;

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

		if (response != null)
			response.close();

		return JSONObject.fromObject(responseStr);

	}
	
	/** Below are primitive methods utilizing OpenNLP **/
	
	public String[] splitSentences(String rawText) {
		return sentenceDetector.sentDetect(rawText);
	}
	

	public String[] tokenize(String sentence) {
		return tokenizer.tokenize(sentence);
	}
	
	public String[] tagPOS(String[] tokenizedSentence) {
		return tagger.tag(tokenizedSentence);
	}

}
