package com.enhype.db;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.enhype.utils.RunConfig;

public class DynamoDB {

	private Logger logger = Logger.getLogger(DynamoDB.class.getName());
	static AmazonDynamoDBClient dynamoDB;

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 */
	public static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentials credentials = new PropertiesCredentials(new File(
				RunConfig.AWSCredentialDir));
		dynamoDB = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDB.setRegion(usWest2);

	}

	public void createTable(String tableName, String tableKey,
			Map<String, String> tableAttrs, long readCapacity,
			long writeCapacity) {

		try {
			
			if ( tableAttrs == null || tableAttrs.keySet().size() == 0 )
				return;

			KeySchemaElement key = new KeySchemaElement().withAttributeName(
					tableKey).withKeyType(KeyType.HASH);

			List<AttributeDefinition> attributes = new ArrayList<AttributeDefinition>();

			ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
					.withReadCapacityUnits(readCapacity)
					.withWriteCapacityUnits(writeCapacity);

			for (String attrName : tableAttrs.keySet()) {
				AttributeDefinition attribute = new AttributeDefinition()
						.withAttributeName(attrName).withAttributeType(
								tableAttrs.get(attrName));
				attributes.add(attribute);
			}

			// Create table if it does not exist yet
			if (Tables.doesTableExist(dynamoDB, tableName)) {
				logger.info("Table " + tableName + " is already ACTIVE");
			} else {
				// Create a table with a primary hash key named 'name', which
				// holds a string
				CreateTableRequest createTableRequest = new CreateTableRequest()
						.withTableName(tableName).withKeySchema(key)
						.withAttributeDefinitions(attributes)
						.withProvisionedThroughput(provisionedThroughput);

				TableDescription createdTableDescription = dynamoDB
						.createTable(createTableRequest).getTableDescription();
				logger.info("Created Table: " + createdTableDescription);

				// Wait for it to become active
				logger.info("Waiting for " + tableName + " to become ACTIVE...");
				Tables.waitForTableToBecomeActive(dynamoDB, tableName);
			}

			// Describe our new table
			DescribeTableRequest describeTableRequest = new DescribeTableRequest()
					.withTableName(tableName);
			TableDescription tableDescription = dynamoDB.describeTable(
					describeTableRequest).getTable();
			logger.info("Table Description: " + tableDescription);

		} catch (AmazonServiceException ase) {
			logAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			logAmazonClientException(ace);
		}
	}

	public void addItem(String tableName, Map<String, String> strAttrs,
			Map<String, List<String>> strSetAttrs) {

		try {

			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

			if (strAttrs != null && strAttrs.keySet().size() != 0)
				for (String strAtr : strAttrs.keySet())
					item.put(strAtr, new AttributeValue(strAttrs.get(strAtr)));

			if (strSetAttrs != null && strSetAttrs.keySet().size() != 0)
				for (String strSetAtr : strSetAttrs.keySet())
					item.put(strSetAtr,
							new AttributeValue(strSetAttrs.get(strSetAtr)));

			PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
			dynamoDB.putItem(putItemRequest);

		} catch (AmazonServiceException ase) {
			logAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			logAmazonClientException(ace);
		}

	}

	public void addNewValueToItem(String tableName, String keyName,
			String keyValue, Map<String, List<String>> strSetAttrUpdates) {

		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();

		if ( strSetAttrUpdates == null || strSetAttrUpdates.keySet().size() == 0 )
			return;
		
		for (String attr : strSetAttrUpdates.keySet())
			updateItems.put(
					attr,
					new AttributeValueUpdate().withAction(AttributeAction.ADD)
							.withValue(new AttributeValue(strSetAttrUpdates.get(attr))));

		updateItem(tableName, keyName, keyValue, updateItems);

	}

	public void addNewAttributeToItem(String tableName, String keyName,
			String keyValue, Map<String, String> attrUpdates) {

		if ( attrUpdates == null || attrUpdates.keySet().size() == 0 )
			return;
		
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		
		for (String attr : attrUpdates.keySet())
			updateItems.put(attr, new AttributeValueUpdate()
					.withValue(new AttributeValue(attrUpdates.get(attr))));

		updateItem(tableName, keyName, keyValue, updateItems);

	}

	public void updateItem(String tableName, String keyName, String keyValue,
			Map<String, AttributeValueUpdate> updateItems) {
		try {

			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put(keyName, new AttributeValue(keyValue));

			UpdateItemRequest updateItemRequest = new UpdateItemRequest()
					.withTableName(tableName)
					.withKey(key)
					.withAttributeUpdates(updateItems);

			dynamoDB.updateItem(updateItemRequest);

		} catch (AmazonServiceException ase) {
			logAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			logAmazonClientException(ace);
		}
	}

	public boolean doesItemExist(String tableName, String keyName, String keyValue) {

		try {

			HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
			key.put(keyName, new AttributeValue(keyValue));
			GetItemRequest getItemRequest = new GetItemRequest().withTableName(
					tableName).withKey(key);

			Map<String, AttributeValue> result = dynamoDB.getItem(
					getItemRequest).getItem();

			// Check the response.
			if( result != null )
				return true;
			else
				return false;

		} catch (AmazonServiceException ase) {
			logAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			logAmazonClientException(ace);
		}
		
		return false;

	}

	public void addBatchItems(String tableName,
			Map<Integer, Map<String, String>> strAttrsList,
			Map<Integer, Map<String, List<String>>> strSetAttrsList) {

		// Create a map for the requests in the batch
		Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
		List<WriteRequest> itemsList = new ArrayList<WriteRequest>();

		try {

			if( strAttrsList == null )
				return;
			
			for (int i : strAttrsList.keySet()) {
				
				// Create a PutRequest for each item
				Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

				Map<String, String> strAttrs = strAttrsList.get(i);
				if ( strAttrs != null )
					for (String strAtr : strAttrs.keySet())
						item.put(strAtr, new AttributeValue(strAttrs.get(strAtr)));
				
				Map<String, List<String>> strSetAttrs = strSetAttrsList.get(i);
				if ( strSetAttrs != null )
					for (String strSetAtr : strSetAttrs.keySet())
						item.put(
								strSetAtr,
								new AttributeValue(strSetAttrs.get(strSetAtr)));

				itemsList.add(new WriteRequest()
						.withPutRequest(new PutRequest().withItem(item)));

			}

			requestItems.put(tableName, itemsList);

			BatchWriteItemResult result;
			BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest()
					.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

			do {
				logger.info("Making the request.");

				batchWriteItemRequest.withRequestItems(requestItems);
				result = dynamoDB.batchWriteItem(batchWriteItemRequest);

				// Print consumed capacity units
				for (ConsumedCapacity consumedCapacity : result
						.getConsumedCapacity()) {
					String table = consumedCapacity.getTableName();
					Double consumedCapacityUnits = consumedCapacity
							.getCapacityUnits();
					logger.info("Consumed capacity units for table " + table
							+ ": " + consumedCapacityUnits);
				}

				// Check for unprocessed keys which could happen if you exceed
				// provisioned throughput
				logger.info("Unprocessed Put and Delete requests: \n"
						+ result.getUnprocessedItems());
				requestItems = result.getUnprocessedItems();

			} while (result.getUnprocessedItems().size() > 0);

		} catch (AmazonServiceException ase) {
			logAmazonServiceException(ase);
		}

	}

	private void logAmazonServiceException(AmazonServiceException ase) {

		logger.error("Caught an AmazonServiceException, which means your request made it "
				+ "to AWS, but was rejected with an error response for some reason.");
		logger.error("Error Message:    " + ase.getMessage());
		logger.error("HTTP Status Code: " + ase.getStatusCode());
		logger.error("AWS Error Code:   " + ase.getErrorCode());
		logger.error("Error Type:       " + ase.getErrorType());
		logger.error("Request ID:       " + ase.getRequestId());

	}

	private void logAmazonClientException(AmazonClientException ace) {

		logger.error("Caught an AmazonClientException, which means the client encountered "
				+ "a serious internal problem while trying to communicate with AWS, "
				+ "such as not being able to access the network.");
		logger.error("Error Message: " + ace.getMessage());

	}
}
