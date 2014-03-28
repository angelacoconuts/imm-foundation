package org.db;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.utils.RunConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class AmazonDynamoDB {

	static AmazonDynamoDBClient dynamoDB;

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 */
	private static void init() throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		AWSCredentials credentials = new PropertiesCredentials(new File(RunConfig.AWSCredentialDir));
		dynamoDB = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDB.setRegion(usWest2);

	}

	public void testConnection() {

		try {
			init();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			
			String tableName = "my-favorite-table";

			// Create table if it does not exist yet
			if (Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is already ACTIVE");
			} else {
				// Create a table with a primary hash key named 'name', which
				// holds a string
				CreateTableRequest createTableRequest = new CreateTableRequest()
						.withTableName(tableName)
						.withKeySchema(
								new KeySchemaElement()
										.withAttributeName("name").withKeyType(
												KeyType.HASH))
						.withAttributeDefinitions(
								new AttributeDefinition().withAttributeName(
										"name").withAttributeType(
										ScalarAttributeType.S))
						.withProvisionedThroughput(
								new ProvisionedThroughput()
										.withReadCapacityUnits(1L)
										.withWriteCapacityUnits(1L));
				TableDescription createdTableDescription = dynamoDB
						.createTable(createTableRequest).getTableDescription();
				System.out.println("Created Table: " + createdTableDescription);

				// Wait for it to become active
				System.out.println("Waiting for " + tableName
						+ " to become ACTIVE...");
				Tables.waitForTableToBecomeActive(dynamoDB, tableName);
			}

			// Describe our new table
			DescribeTableRequest describeTableRequest = new DescribeTableRequest()
					.withTableName(tableName);
			TableDescription tableDescription = dynamoDB.describeTable(
					describeTableRequest).getTable();
			System.out.println("Table Description: " + tableDescription);

			// Add an item
			Map<String, AttributeValue> item = newItem(
					"Bill & Ted's Excellent Adventure", 1989, "****", "James",
					"Sara");
			PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
			PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
							+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out
					.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with AWS, "
							+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static Map<String, AttributeValue> newItem(String name, int year,
			String rating, String... fans) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("name", new AttributeValue(name));
		item.put("year", new AttributeValue().withN(Integer.toString(year)));
		item.put("rating", new AttributeValue(rating));
		item.put("fans", new AttributeValue().withSS(fans));

		return item;
	}
}
