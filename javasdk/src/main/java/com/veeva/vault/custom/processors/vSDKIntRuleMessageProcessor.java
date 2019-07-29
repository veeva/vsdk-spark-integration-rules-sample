package com.veeva.vault.custom.processors;

import com.veeva.vault.sdk.api.core.LogService;
import com.veeva.vault.sdk.api.core.ServiceLocator;
import com.veeva.vault.sdk.api.core.ValueType;
import com.veeva.vault.sdk.api.core.VaultCollections;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.http.HttpMethod;
import com.veeva.vault.sdk.api.http.HttpRequest;
import com.veeva.vault.sdk.api.http.HttpResponseBodyValueType;
import com.veeva.vault.sdk.api.http.HttpService;
import com.veeva.vault.sdk.api.integration.FieldRule;
import com.veeva.vault.sdk.api.integration.FieldRuleResult;
import com.veeva.vault.sdk.api.integration.IntegrationRule;
import com.veeva.vault.sdk.api.integration.IntegrationRuleService;
import com.veeva.vault.sdk.api.job.JobParameters;
import com.veeva.vault.sdk.api.job.JobService;
import com.veeva.vault.sdk.api.json.*;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.queue.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * This is a Vault Java SDK Integration Rule MessageProcessor that demonstrates a Spark vault to vault messaging queue.
 *
 * On the target vault, the MessageProcessor processes `Message` records received in the inbound queue - `vsdk_warranty_in_queue__c`.
 *    - Use the integration rules to map data from the source vault to the target vault
 *    - Use the `HttpService` to run a query against the source vault for more data
 *
 */

@MessageProcessorInfo()
public class vSDKIntRuleMessageProcessor implements MessageProcessor {

	public void execute(MessageContext context) {

		//Processes a message from the queue and makes a HTTP callback to the source vault.
		//Here we want to grab non-empty messages and make sure that the message has a `vsdk_warranty__c` attribute.
		//Once a record is found, an API VQL query is made to the source vault to grab data from the source record.
		//This queried information is added to the target record.
		//After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
		//     `successfully_processed__c` - if the query and update are successful
		//     `processing_failed__c`       - if the query or update are unsuccessful
		Message message = context.getMessage();
		if (!message.getMessageItems().isEmpty() &&
				message.getAttribute("object", MessageAttributeValueType.STRING).toString().contains("vsdk_warranty__c")) {
			callbackMessageIntRules(message, context);
		}
	}

	//Processes a message from the queue and makes a HTTP callback to the source vault.
	//This processing searches for an existing record based on the incoming message which has the source record ID.
	//Integration rules are used to determine source and target Vault field mappings.
	//Once a record is found, an API VQL query is made to the source vault to grab data from the source record.
	//This queried information is added to the target record.
	//After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
	//     `successfully_processed__c` - if the query and update are successful
	//     `processing_failed__c`       - if the query or update are unsuccessful
	public static void callbackMessageIntRules(Message message, MessageContext context) {

		LogService logService = ServiceLocator.locate(LogService.class);
		RecordService recordService = ServiceLocator.locate(RecordService.class);
		QueryService queryService = ServiceLocator.locate(QueryService.class);

		final String destinationObject = "vsdk_claims_warranty__c";

		List<Record> processingRecordList = VaultCollections.newList();
		Map<String,Record> callBackMessageMap = VaultCollections.newMap();
		Map<String,String> incomingMessageMap = VaultCollections.newMap();

		//Retrieve all items in the Message and put them in a Map.
		//The map is used to determine if the code wasn't able to find existing records.
		for (String sourceRecordId : message.getMessageItems()) {
			incomingMessageMap.put(sourceRecordId, "true");
			logService.info("Incoming message item: " + sourceRecordId);
		}

		//Query to see any incoming IDs match to any existing vsdk_claims_warranty__c records.
		String query = "select id, source_record__c from " + destinationObject
				+ " where source_record__c contains ('" + String.join("','", incomingMessageMap.keySet())  + "')";
		QueryResponse queryResponse = queryService.query(query);

		logService.info("Query: " + query);

		queryResponse.streamResults().forEach(qr -> {
			String id = qr.getValue("id", ValueType.STRING);
			String source_id = qr.getValue("source_record__c", ValueType.STRING);
			logService.info("Found existing record with ID: " + id);

			// Add the existing record for callback
			Record recordUpdate = recordService.newRecordWithId(destinationObject, id);
			callBackMessageMap.put(source_id, recordUpdate);

			// Remove the record from the incoming map so it doesn't get recreated
			incomingMessageMap.remove(source_id);
		});
		queryResponse = null;

		// Add the new records for callback
		for (String key : incomingMessageMap.keySet()) {
			Record newSparkRecord = recordService.newRecord(destinationObject);
			newSparkRecord.setValue("source_vault__c", context.getRemoteVaultId().toString());
			callBackMessageMap.put(key, newSparkRecord);
			logService.info("New incoming message item: " + key);
		}
		incomingMessageMap.clear();

		//Run a `HttpService` query callout.
		//This retrieves additional data from the source vault and populates the data into the newSparkRecords.
		//Add the updated records to a list that is processed with `batchSaveRecords`
		if (callBackMessageMap.size() > 0) {
			com.veeva.vault.custom.processors.vSDKIntRuleMessageProcessor.HttpCallouts.v2vHttpQueryIntRules(callBackMessageMap, context.getConnectionId());
			processingRecordList.addAll(callBackMessageMap.values());
			callBackMessageMap.clear();
		}

		// Save the records to the target vault
		if (processingRecordList.size() > 0) {
			recordService.batchSaveRecords(processingRecordList).onSuccesses(successMessage -> {

				List<Record> successfulRecords = VaultCollections.newList();
				successMessage.stream().forEach(positionalRecordId -> {
					Record record = recordService.newRecordWithId(destinationObject, positionalRecordId.getRecordId());

					successfulRecords.add(record);
				});

			}).onErrors(batchOperationErrors -> {
				batchOperationErrors.stream().findFirst().ifPresent(error -> {
					String errMsg = error.getError().getMessage();
					int errPosition = error.getInputPosition();
					String name = processingRecordList.get(errPosition).getValue("name__v", ValueType.STRING);

					logService.info("Unable to create '" + processingRecordList.get(errPosition).getObjectName() + "' record: '" +
							name + "' because of '" + errMsg + "'.");
				});
			}).execute();

			logService.info(message.toString());
		}
	}


	//Inner public class created to implement Vault Java SDK Http Callouts.
	public static class HttpCallouts {
		public HttpCallouts() {}

		//Input recordsToCheck Map contains a key (source ID) : value (record being updated) pair.
		//The keys are used to query the source system for field information.
		//The values are used to update the target records with the retrieved field information.
		public static void v2vHttpQueryIntRules(Map<String,Record> recordsToCheck, String connectionId) {

			LogService logService = ServiceLocator.locate(LogService.class);

			//This is a vault to vault Http Request to the connection `vsdk_connection_to_warranties`
			HttpService httpService = ServiceLocator.locate(HttpService.class);
			HttpRequest request = httpService.newHttpRequest("vsdk_connection_to_warranties");

			// Instantiate the Integration Rule Service
			IntegrationRuleService integrationRuleService = ServiceLocator.locate(IntegrationRuleService.class);

			// Create a query from the query object/fields for the configured
			// Integration Rule, i.e. "integration_rule_1__c"
			//
			String query = "";
			Collection<IntegrationRule> intRule = (integrationRuleService.getIntegrationRules(connectionId));
			for (Iterator<IntegrationRule> iterator = intRule.iterator(); iterator.hasNext();) {
				IntegrationRule intRuleResult = iterator.next();
				String intRuleName = intRuleResult.getName(); // integration_rule_1__c
				Boolean intRuleIsActive = intRuleResult.isActive(); // true
				if ((intRuleName.equals("warranty_int_rule_1__c")) && (intRuleIsActive)) {
					String queryObject = intRuleResult.getFieldRules().iterator().next().getQueryObject();
					Collection<String> queryFields = intRuleResult.getQueryFields(queryObject); // i.e. "vsdk_warranty__c"
					query =  "select "
							+ String.join(",", queryFields)
							+ ", (select name__v, period_in_months__c from warranty_period__cr)"
							+ " from " + queryObject
							+ " where id contains ('" + String.join("','", recordsToCheck.keySet()) + "')";
					break;
				}
			}
			intRule = null;

			// Alternatively queries can be created manually
			// e.g.:
			//			String query = "select id, "
			//					+ "name__v, "
			//					+ "customer_name__c, "
			//					+ "cover_start_date__c, "
			//					+ "extended_cover__c, "
			//					+ "manufacturer__c, "
			//					+ "(select name__v, period_in_months__c from warranty_period__cr) "
			//					+ "from vsdk_warranty__c where id contains ('" + String.join("','", recordsToCheck.keySet()) + "')";


			//The configured connection provides the full DNS name.
			//For the path, you only need to append the API endpoint after the DNS.
			//The query endpoint takes a POST where the BODY is the query itself.
			request.setMethod(HttpMethod.POST);
			request.appendPath("/api/v19.1/query");
			request.setHeader("Content-Type", "application/x-www-form-urlencoded");
			request.setHeader("X-VaultAPI-DescribeQuery", "true"); // *** New
			request.setBodyParam("q", query);

			//Send the request the source vault. The response received back should be a JSON response.
			//First, the response is parsed into a `JsonData` object
			//From the response, the `getJsonObject()` will get the response as a parseable `JsonObject`
			//    * Here the `getValue` method can be used to retrieve `responseStatus`, `responseDetails`, and `data`
			//The `data` element is an array of JSON data. This is parsed into a `JsonArray` object.
			//    * Each queried record is returned as an element of the array and must be parsed into a `JsonObject`.
			//    * Individual fields can then be retrieved from each `JsonObject` that is in the `JsonArray`.
			httpService.send(request, HttpResponseBodyValueType.JSONDATA)
					.onSuccess(httpResponse -> {

						JsonData response = httpResponse.getResponseBody();

						if (response.isValidJson()) {
							String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

							if (responseStatus.equals("SUCCESS")) {
								IntegrationRule integrationRule = (integrationRuleService.getIntegrationRules(connectionId)).iterator().next(); // *** New
								JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
								JsonObject queryDescribe = response.getJsonObject().getValue("queryDescribe", JsonValueType.OBJECT); // *** New

								logService.info("HTTP Query Request: SUCCESS");

								//Retrieve each record returned from the VQL query.
								//Each element of the returned `data` JsonArray is a record with it's queried fields.
								for (int i = 0; i < data.getSize();i++) {
									JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);

									String sourceId = queryRecord.getValue("id", JsonValueType.STRING);

									// Get the resulting values for a row based on the evaluation of the rule against a json query response record
									Collection<FieldRuleResult> fieldRuleResults = integrationRuleService.evaluateFieldRules(integrationRule, "vsdk_claims_warranty__c", queryDescribe, queryRecord);

									// Set the records field values in the target vault using values in "fieldRuleResults"
									for (Iterator<FieldRuleResult> iterator = fieldRuleResults.iterator(); iterator.hasNext();) {
										FieldRuleResult fieldRuleResult = iterator.next();
										recordsToCheck.get(sourceId).setValue(fieldRuleResult.getTargetField(), fieldRuleResult.getValue());
									}

									//
									// Set any additional custom fields
									//

									// If desired query results can be used to calculate custom fields.
									// For instance the period_in_months__c field within the warranty_period__cr subquery
									// can be retrieved via a JsonObject then JsonArray.
									// This is basically an embedded JSON response.
									JsonObject subquery = queryRecord.getValue("warranty_period__cr", JsonValueType.OBJECT);
									JsonArray subqueryData = subquery.getValue("data", JsonValueType.ARRAY);
									JsonObject subqueryRecord = subqueryData.getValue(0, JsonValueType.OBJECT);
									Integer warrantyPeriodMonths = Integer.parseInt(subqueryRecord.getValue("period_in_months__c", JsonValueType.STRING));

									//Long warrantyPeriodMonths = new Long(warrantyPeriodMonthsInt);
									// Use of method [<init>] in class [java.lang.Long] at line [265] is not allowed

									// Now calculate the end_date__c by adding the warranty period minus a day to the start_date__c
									LocalDate startDate = recordsToCheck.get(sourceId).getValue("start_date__c", ValueType.DATE);
									recordsToCheck.get(sourceId).setValue("end_date__c", startDate.plusMonths(warrantyPeriodMonths).minusDays(1));

									// List values can also be populated, such as the integration status in this example.
									recordsToCheck.get(sourceId).setValue("integration_status__c", VaultCollections.asList("processing_successful__c"));

								}
								data = null;
							}
							else {
								for (String key : recordsToCheck.keySet()) {
									recordsToCheck.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
								}
							}
							response = null;
						}
						else {
							logService.info("v2vHttpUpdate error: Received a non-JSON response.");
						}
					})
					.onError(httpOperationError -> {

						logService.info("v2vHttpUpdate error: httpOperationError.");

						for (String key : recordsToCheck.keySet()) {
							recordsToCheck.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
						}
					}).execute();

			request = null;

		}

	}
}
