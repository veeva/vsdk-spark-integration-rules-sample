package com.veeva.vault.custom.processors;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.http.HttpMethod;
import com.veeva.vault.sdk.api.http.HttpRequest;
import com.veeva.vault.sdk.api.http.HttpResponseBodyValueType;
import com.veeva.vault.sdk.api.http.HttpService;
import com.veeva.vault.sdk.api.integration.FieldRuleResult;
import com.veeva.vault.sdk.api.integration.IntegrationRule;
import com.veeva.vault.sdk.api.integration.IntegrationRuleService;
import com.veeva.vault.sdk.api.json.*;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;
import com.veeva.vault.sdk.api.queue.*;

import java.time.LocalDate;
import java.util.*;

/**
 * 
 * This is a Vault Java SDK MessageProcessor that demonstrates using Spark Integration Rules to extract, transform and load data
 * from a source vault to a target vault, for any messages on Spark vault to vault messaging queue.
 * 
 * On the target vault, the MessageProcessor processes `Message` records received in the inbound queue - `vsdk_warranty_in_queue__c`.
 *    - `Integration rules` are used to map data from the source vault to the target vault
 *    - The `HttpService` is used to run a query against the source vault for more data
 *
 */

@MessageProcessorInfo()
public class vSDKIntRuleMessageProcessor implements MessageProcessor {

	public void execute(MessageContext context) {

        //Processes a message from the queue
    	//Here we want to grab non-empty messages and make sure that the message has a `vsdk_warranty__c` attribute.
        //Once a record is found, an HTTP callback is made to the source vault to grab data from the source record.
        //This queried information is added to the target record.
        //After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
        //     `successfully_processed__c` - if the query and update are successful
        //     `processing_failed__c`       - if the query or update are unsuccessful
		Message message = context.getMessage();
		if (!message.getMessageItems().isEmpty() &&
            message.getAttribute("object", MessageAttributeValueType.STRING).toString().contains("vsdk_warranty__c")) {
			intRulesETLWarrantiesMessage(message, context);
        }
    }

	//Processes a message from the queue.
	//This processing searches for an existing record based on the incoming message which has the source record ID.
	//Integration rules are used to determine source and target Vault field mappings.
	//Once a record is found, an API VQL query is made to the source vault to grab data from the source record.
	//This queried information is added to the target record.
	//After it is updated, an API record update to sent to the source vault to update the `integration_status__c`:
	//     `successfully_processed__c` - if the query and update are successful
	//     `processing_failed__c`       - if the query or update are unsuccessful
	public static void intRulesETLWarrantiesMessage(Message message, MessageContext context) {

		LogService logService = ServiceLocator.locate(LogService.class);
		RecordService recordService = ServiceLocator.locate(RecordService.class);
		QueryService queryService = ServiceLocator.locate(QueryService.class);

		final String destinationObject = "vsdk_claims_warranty__c";

		List<Record> transformedRecordList = VaultCollections.newList();
		Map<String,Record> etlMessageMap = VaultCollections.newMap();
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
			etlMessageMap.put(source_id, recordUpdate);

			// Remove the record from the incoming map so it doesn't get recreated
			incomingMessageMap.remove(source_id);
		});
		queryResponse = null;

		// Add the new records for callback
		for (String key : incomingMessageMap.keySet()) {
			Record newSparkRecord = recordService.newRecord(destinationObject);
			newSparkRecord.setValue("source_vault__c", context.getRemoteVaultId().toString());
			etlMessageMap.put(key, newSparkRecord);
			logService.info("New incoming message item: " + key);
		}
		incomingMessageMap.clear();

		// Run a `HttpService` query callout to the Source Vault.
		// This retrieves additional data from the source vault and populates the data into the newSparkRecords.
		// Add the updated records to a list that is processed with `batchSaveRecords`
		if (etlMessageMap.size() > 0) {

            HttpCallouts.v2vIntRulesExtractAndTransformWarranties(etlMessageMap, context.getConnectionId(), destinationObject);
         	transformedRecordList.addAll(etlMessageMap.values());
			etlMessageMap.clear();
		}

		// Save the records to the target vault
		if (transformedRecordList.size() > 0) {
			recordService.batchSaveRecords(transformedRecordList).onSuccesses(successMessage -> {

				List<Record> successfulRecords = VaultCollections.newList();
				successMessage.stream().forEach(positionalRecordId -> {
					Record record = recordService.newRecordWithId(destinationObject, positionalRecordId.getRecordId());

					successfulRecords.add(record);
				});

			}).onErrors(batchOperationErrors -> {
				batchOperationErrors.stream().findFirst().ifPresent(error -> {
					String errMsg = error.getError().getMessage();
					int errPosition = error.getInputPosition();
					String name = transformedRecordList.get(errPosition).getValue("name__v", ValueType.STRING);

					logService.info("intRulesETLWarrantiesMessage error: Unable to create '" + transformedRecordList.get(errPosition).getObjectName() + "' record: '" +
							name + "' because of '" + errMsg + "'.");
				});
			}).execute();

			logService.info(message.toString());
		}
	}

    //Inner public class created to implement Vault Java SDK Http Callouts.
    public static class HttpCallouts {
    	public HttpCallouts() {}

        //Input recordsToExtractAndTransform Map contains a key (source ID) : value (record being updated) pair.
        //The keys are used to query the source system for field information.
        //The values are used to update the target records with the retrieved field information.
        public static void v2vIntRulesExtractAndTransformWarranties(Map<String,Record> recordsToExtractAndTransform, String connectionId, String destinationObject) {

            LogService logService = ServiceLocator.locate(LogService.class);

            //This is a vault to vault Http Request to the connection `vsdk_connection_to_warranties`
            HttpService httpService = ServiceLocator.locate(HttpService.class);
            HttpRequest request = httpService.newHttpRequest("vsdk_connection_to_warranties");

            // Instantiate the Integration Rule Service
            IntegrationRuleService integrationRuleService = ServiceLocator.locate(IntegrationRuleService.class);

            // Create a query from the query object/fields for the configured
            // Integration Rule, i.e. "warranty_int_rule_1__c"
            //
            String query = "";
            Collection<IntegrationRule> intRules = (integrationRuleService.getIntegrationRules(connectionId));
            for (Iterator<IntegrationRule> intRulesiterator = intRules.iterator(); intRulesiterator.hasNext();) {
                IntegrationRule intRuleResult = intRulesiterator.next();
                String intRuleName = intRuleResult.getName();
                // Process the rule if the rule exists and has a status of active.
                Boolean intRuleIsActive = intRuleResult.isActive();
                if ((intRuleName.equals("warranty_int_rule_1__c")) && (intRuleIsActive)) {
                    String queryObject = intRuleResult.getFieldRules().iterator().next().getQueryObject();
                    Collection<String> queryFields = intRuleResult.getQueryFields(queryObject); // i.e. "vsdk_warranty__c"
                    query =  "SELECT "
                            + String.join(",", queryFields)
                            // Manually add an custom fields not included in the integration rules
                            + ", (SELECT name__v, period_in_months__c FROM warranty_period__cr)"
                            + " FROM " + queryObject
                            + " WHERE id CONTAINS ('" + String.join("','", recordsToExtractAndTransform.keySet()) + "')";

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
                    request.appendPath("/api/v19.3/query");
                    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
                    request.setHeader("X-VaultAPI-DescribeQuery", "true"); // *** New
                    request.setBodyParam("q", query);

                    //Send the request the source vault via a callback. The response received back should be a JSON response.
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

                                        JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                                        JsonObject queryDescribe = response.getJsonObject().getValue("queryDescribe", JsonValueType.OBJECT);

                                        logService.info("HTTP Query Request: SUCCESS");

                                        //Retrieve each record returned from the VQL query.
                                        //Each element of the returned `data` JsonArray is a record with it's queried fields.
                                        for (int i = 0; i < data.getSize();i++) {
                                            JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);

                                            String sourceId = queryRecord.getValue("id", JsonValueType.STRING);

                                            // Get the resulting values for a row based on the evaluation of the rule against a json query response record
                                            Collection<FieldRuleResult> fieldRuleResults = integrationRuleService.evaluateFieldRules(intRuleResult, destinationObject, queryDescribe, queryRecord);

                                            // Set the records field values in the target vault using values in "fieldRuleResults"
                                            for (Iterator<FieldRuleResult> fieldRuleResultIterator = fieldRuleResults.iterator(); fieldRuleResultIterator.hasNext(); ) {
                                                FieldRuleResult fieldRuleResult = fieldRuleResultIterator.next();
                                                recordsToExtractAndTransform.get(sourceId).setValue(fieldRuleResult.getTargetField(), fieldRuleResult.getValue());
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

                                            // Now calculate the end_date__c by adding the warranty period minus a day to the start_date__c
                                            LocalDate startDate = recordsToExtractAndTransform.get(sourceId).getValue("start_date__c", ValueType.DATE);
                                            recordsToExtractAndTransform.get(sourceId).setValue("end_date__c", startDate.plusMonths(warrantyPeriodMonths).minusDays(1));

                                            // List values can also be populated, such as the integration status in this example.
                                            recordsToExtractAndTransform.get(sourceId).setValue("integration_status__c", VaultCollections.asList("processing_successful__c"));

                                        }
                                        data = null;
                                    }
                                    else {
                                        for (String key : recordsToExtractAndTransform.keySet()) {
                                            recordsToExtractAndTransform.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
                                        }
                                    }
                                    response = null;
                                }
                                else {
                                    logService.info("v2vIntRulesExtractAndTransformWarranties error: Received a non-JSON response.");
                                }
                            })
                            .onError(httpOperationError -> {

                                logService.info("v2vIntRulesExtractAndTransformWarranties error: httpOperationError.");

                                for (String key : recordsToExtractAndTransform.keySet()) {
                                    recordsToExtractAndTransform.get(key).setValue("integration_status__c", VaultCollections.asList("processing_failed__c"));
                                }
                            }).execute();

                    request = null;
                    break;

                } else {

                    logService.info("v2vIntRulesExtractAndTransformWarranties error: No active version of integration rule 'warranty_int_rule_1__c' is configured.");

                }

                intRules = null;

            }
        }
	}
}
