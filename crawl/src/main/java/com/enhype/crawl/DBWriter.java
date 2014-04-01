package com.enhype.crawl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.enhype.db.DynamoDB;
import com.enhype.model.WebPage;
import com.enhype.utils.RunConfig;

public class DBWriter {

	private DynamoDB dynamo;
	private Logger logger = Logger.getLogger(DBWriter.class.getName());

	private static String entityTableName = "Entities";
	private static String pageTableName = "Pages";
	private static String siteTableName = "Sites";
	private static String sentenceTableName = "Sentences";
	private static String entityTableKey = "uri";
	private static String entityIndexKey = "bucket";
	private static String siteTableKey = "entry";
	private static String pageTableKey = "id";
	private static String sentenceTableKey = "id";

	public DBWriter() {

		dynamo = new DynamoDB();

		try {
			DynamoDB.init();
		} catch (Exception e) {
			logger.error("Failing to connection to DynamoDB", e);
		}

	}

	public void createSiteTable() {

		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(siteTableKey, "S");

		dynamo.createTable(siteTableName, siteTableKey, tableAttrs,
				RunConfig.BASE_READ_CAPACITY, RunConfig.BASE_WRITE_CAPACITY);

	}
	
	public void createPageTable() {

		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(pageTableKey, "S");

		dynamo.createTable(pageTableName, pageTableKey, tableAttrs,
				RunConfig.BASE_READ_CAPACITY, RunConfig.BASE_WRITE_CAPACITY);

	}

	public void createSentenceTable() {

		long writeCapacity = RunConfig.SENT_WRITE_CAPACITY;
		long readCapacity = RunConfig.SENT_READ_CAPACITY;
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(sentenceTableKey, "S");

		dynamo.createTable(sentenceTableName, sentenceTableKey, tableAttrs,
				readCapacity, writeCapacity);

	}

	public void createEntityTable() {

		long writeCapacity = RunConfig.ENTITY_WRITE_CAPACITY;
		long readCapacity = RunConfig.ENTITY_READ_CAPACITY;
		Map<String, String> tableAttrs = new HashMap<String, String>();
		tableAttrs.put(entityTableKey, "S");
		tableAttrs.put(entityIndexKey, "S");

		dynamo.createTable(entityTableName, entityTableKey, entityIndexKey, tableAttrs,
				readCapacity, writeCapacity);

	}

	public void writeSite(String entry, long startPageSeq, long endPageSeq) {

		Map<String, String> siteAttrs = new HashMap<String, String>();
		Map<String, List<String>> siteSetAttrs = new HashMap<String, List<String>>();

		siteAttrs.put(siteTableKey, entry);
		siteAttrs.put("start", Long.toString(startPageSeq));
		siteAttrs.put("end", Long.toString(endPageSeq));

		dynamo.addItem(siteTableName, siteAttrs, siteSetAttrs);

	}
	
	public void writePage(WebPage page) {

		Map<String, String> pageAttrs = new HashMap<String, String>();
		Map<String, List<String>> pageSetAttrs = new HashMap<String, List<String>>();

		pageAttrs.put(pageTableKey, page.getId());
		pageAttrs.put("url", page.getURL());
		pageAttrs.put("title", page.getTitle());
		pageAttrs.put("fetch_time", page.getFetchTime());

		if (page.getKeywords() != null && page.getKeywords().length() > 0)
			pageSetAttrs.put("keywords",
					Arrays.asList(StringUtils.split(page.getKeywords(), ",")));

		dynamo.addItem(pageTableName, pageAttrs, pageSetAttrs);

	}

	public void writeSentences(WebPage page) {

		logger.info("Writing " + page.getSentencePositionMap().keySet().size()
				+ " sentences. ");

		for (int sentPos : page.getSentencePositionMap().keySet()) {

			if (page.getSentenceAdjMap().containsKey(sentPos)
					|| page.getSentenceEntityMap().containsKey(sentPos)) {

				Map<String, String> sentAttrs = new HashMap<String, String>();
				Map<String, List<String>> sentSetAttrs = new HashMap<String, List<String>>();
				String sentenceId = page.getId() + "||" + sentPos;

				sentAttrs.put(sentenceTableKey, sentenceId);
				sentAttrs.put("text", page.getSentencePositionMap()
						.get(sentPos));

				if (page.getSentenceNounMap().containsKey(sentPos))
					sentSetAttrs.put("nouns", new ArrayList(page
							.getSentenceNounMap().get(sentPos)));
				if (page.getSentenceAdjMap().containsKey(sentPos))
					sentSetAttrs.put("adjs", new ArrayList(page
							.getSentenceAdjMap().get(sentPos)));
				if (page.getSentenceEntityMap().containsKey(sentPos))
					sentSetAttrs.put("entities", new ArrayList(page
							.getSentenceEntityMap().get(sentPos)));

				dynamo.addItem(sentenceTableName, sentAttrs, sentSetAttrs);

			}

		}

	}

	public void writeSentencesBatch(WebPage page) {

		logger.info("Writing " + page.getSentencePositionMap().keySet().size()
				+ " sentences. ");

		Map<Integer, Map<String, String>> strAttrsList = new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, List<String>>> strSetAttrsList = new HashMap<Integer, Map<String, List<String>>>();

		List<Integer> sents = new ArrayList(page.getSentencePositionMap()
				.keySet());
		int batchSize = 5;
		int numOfRequests = sents.size() / batchSize + 1;

		for (int sent = 0, index = 0; index < numOfRequests; index++) {

			strAttrsList.clear();
			strSetAttrsList.clear();

			for (int i = 0; i < batchSize && sent < sents.size(); i++, sent++) {

				int sentPos = sents.get(sent);

				Map<String, String> sentAttrs = new HashMap<String, String>();
				Map<String, List<String>> sentSetAttrs = new HashMap<String, List<String>>();
				String sentenceId = page.getId() + "||" + sentPos;

				sentAttrs.put(sentenceTableKey, sentenceId);
				sentAttrs.put("domain", RunConfig.CRAWL_DOMAIN_SEQ);
				sentAttrs.put("text", page.getSentencePositionMap()
						.get(sentPos));

				if (page.getSentenceNounMap().containsKey(sentPos))
					sentSetAttrs.put("nouns", new ArrayList(page
							.getSentenceNounMap()
							.get(sentPos)));
				if (page.getSentenceAdjMap().containsKey(sentPos))
					sentSetAttrs.put("adjs", new ArrayList(page
							.getSentenceAdjMap()
							.get(sentPos)));
				if (page.getSentenceEntityMap().containsKey(sentPos))
					sentSetAttrs.put("entities", new ArrayList(page
							.getSentenceEntityMap()
							.get(sentPos)));

				strAttrsList.put(sentPos, sentAttrs);
				strSetAttrsList.put(sentPos, sentSetAttrs);

			}

			dynamo.addBatchItems(sentenceTableName, strAttrsList,
					strSetAttrsList);
		}

	}

	public void writeEntities(WebPage page) {

		logger.info("Writing " + page.getEntityMentionMap().keySet().size()
				+ " entities. ");

		for (String entityURI : page.getEntityMentionMap().keySet()) {

			Map<String, List<String>> entityUpdates = new HashMap<String, List<String>>();
			entityUpdates.put("mentions", new ArrayList(page
					.getEntityMentionMap().get(entityURI)));
			entityUpdates.put("surfaces", new ArrayList(
					Crawler.entitiesSurfaceList.get(entityURI)));

			if (!Crawler.knownEntities.contains(entityURI)) {

				boolean exist = dynamo.doesItemExist(entityTableName,
						entityTableKey, entityURI);

				if (!exist) {

					Map<String, String> entityAttrs = new HashMap<String, String>();

					entityAttrs.put(entityTableKey, entityURI);
					entityAttrs.put(entityIndexKey, Integer.toString(1+(int)(Math.random()*10)) );
					if (Crawler.entitiesList.get(entityURI).length() > 0)
						entityUpdates.put("types",
								Arrays.asList(StringUtils.split(
										Crawler.entitiesList.get(entityURI),
										",")));

					dynamo.addItem(entityTableName, entityAttrs, entityUpdates);
					Crawler.knownEntities.add(entityURI);

				} else {
					dynamo.addNewValueToItem(entityTableName, entityTableKey,
							entityURI, entityUpdates);
				}

			} else {
				dynamo.addNewValueToItem(entityTableName, entityTableKey,
						entityURI, entityUpdates);
			}

		}

	}

}
