package com.veeva.vault.custom.udc;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.http.HttpMethod;
import com.veeva.vault.sdk.api.http.HttpRequest;
import com.veeva.vault.sdk.api.http.HttpResponseBodyValueType;
import com.veeva.vault.sdk.api.http.HttpService;
import com.veeva.vault.sdk.api.integration.*;
import com.veeva.vault.sdk.api.json.JsonArray;
import com.veeva.vault.sdk.api.json.JsonData;
import com.veeva.vault.sdk.api.json.JsonObject;
import com.veeva.vault.sdk.api.json.JsonValueType;
import com.veeva.vault.sdk.api.query.QueryResponse;
import com.veeva.vault.sdk.api.query.QueryService;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/******************************************************************************
 * User-Defined Class:  vSDKIntRulesETLWarranties
 * Author:              John Tanner @ Veeva
 * Date:                2019-03-19
 *-----------------------------------------------------------------------------
 * Description: Provides a sample UDC with Spark Integration Rules for extracting
 *              transforming and processing Spark Messages within a target vault.
 *
 *              This message processing is done within it's own UDC, so any
 *              messages which have failed with user exceptions can be re-executed
 *              once the issue has been manually resolved by running a local
 *              user action.
 *
 *              In this example:
 *              - `Integration rules` are used to map data from the source vault
 *                to the target vault
 *              - The `HttpService` is used to run a query against the source
 *                vault for more data
 *
 *-----------------------------------------------------------------------------
 * Copyright (c) 2020 Veeva Systems Inc.  All Rights Reserved.
 *      This code is based on pre-existing content developed and
 *      owned by Veeva Systems Inc. and may only be used in connection
 *      with the deliverable with which it was provided to Customer.
 *--------------------------------------------------------------------
 *
 *******************************************************************************/

@UserDefinedClassInfo()
public class vSDKIntRulesETLWarranties {

    /**
     * Processes the incoming list of warranty records using Integration Rules.
     * For each record...
     *  - Extract the source values using a callback
     *  - Transform the data
     *  - Update the target record
     *  - On error an integration exception is raised
     *
     * @param incomingMessageList List contains the ID of all the records to process.
     * @param sourceVaultId for the v2v connection
     * @param sourceObject for the v2v connection
     * @param connectionId of the v2v connection
     * @param connectionName of the v2v connection
     * @param integrationPointApiName of the v2v connection
     */
    public static void process(List<String> incomingMessageList,
                               String sourceVaultId,
                               String sourceObject,
                               String connectionId,
                               String connectionName,
                               String integrationPointApiName) {

        final String destinationObject = "vsdk_claims_warranty__c";
        final String integrationApiName = "warranties__c";

        // Create a map of Messages to be processed via ETL.
        // Any records that already exist in the target Vault will be updated, whereas
        // new records will be created
        Map<String,Record> etlMessageMap = populateETLMessageMap(incomingMessageList,
                destinationObject,
                sourceVaultId);

        // Use integration rules to extract source data, and transform it into the target value
        // This retrieves additional data from the source Vault through VQL and HTTP Callout and
        // populates the data into a target records list "transformedRecordList", which are
        // saved to the target vault with `batchSaveRecords`.
        if (etlMessageMap.size() > 0) {

            v2vIntRulesETLWarrantiesFromMessageMap(etlMessageMap,
                    connectionId,
                    connectionName,
                    sourceObject,
                    destinationObject,
                    integrationApiName,
                    integrationPointApiName);
        }

    }

    /**
     * Creates a Message Map of Records to ETL, comprising existing records for
     * updated and new records for creation.
     *
     * @param incomingMessageList List contains the ID of all the records to process.
     * @param destinationObject of the v2v connection
     * @param sourceVaultId for the v2v connection
     */
    private static Map<String,Record> populateETLMessageMap(List<String> incomingMessageList,
                                                            String destinationObject,
                                                            String sourceVaultId) {

        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);

        Map<String,Record> etlMessageMap = VaultCollections.newMap();

        // Query to see any incoming IDs match to any existing vsdk_claims_warranty__c records.
        StringBuilder query = new StringBuilder();
        query.append("SELECT id, source_record__c ");
        query.append("FROM ").append(destinationObject).append(" ");
        query.append("WHERE source_record__c contains ('" + String.join("','", incomingMessageList)  + "')");

        QueryResponse queryResponse = queryService.query(query.toString());

        logService.info("Query existing records by ID: " + query);

        // Any records that already exist in the target Vault will be updated
        queryResponse.streamResults().forEach(qr -> {
            String id = qr.getValue("id", ValueType.STRING);
            String source_id = qr.getValue("source_record__c", ValueType.STRING);
            logService.info("Found existing record with ID: " + id);

            // Add the existing record for callback
            Record recordUpdate = recordService.newRecordWithId(destinationObject, id);
            etlMessageMap.put(source_id, recordUpdate);

            // Remove the record from the incoming map so it doesn't get recreated
            //incomingMessageMap.remove(source_id);
            incomingMessageList.remove(source_id);
        });
        queryResponse = null;

        // Add the new records to "etlMessageMap" for callback
        for (String key : incomingMessageList) {
            Record newSparkRecord = recordService.newRecord(destinationObject);
            newSparkRecord.setValue("source_vault__c", sourceVaultId);
            etlMessageMap.put(key, newSparkRecord);
            logService.info("New incoming message item: " + key);
        }
        incomingMessageList.clear();

        return etlMessageMap;
    }

    /**
     * The integration rules are used to query the source vault for field information.
     * These are then transformed using the integration rule field mappings, which are
     * in turn used to update the target records.
     *
     * @param recordsToETL Map contains a key (source ID) : value (record being updated) pair.
     * @param connectionId of the v2v connection
     * @param connectionName of the v2v connection
     * @param sourceObject for the v2v connection
     * @param destinationObject for the v2v connection
     * @param integrationApiName for the v2v connection
     * @param integrationPointApiName for the v2v connection
     */
    //Input recordsToExtractAndTransform Map contains a key (source ID) : value (record being updated) pair.
    //The keys are used to query the source system for field information.
    //The values are used to update the target records with the retrieved field information.
    private static void v2vIntRulesETLWarrantiesFromMessageMap(Map<String, Record> recordsToETL,
                                                               String connectionId,
                                                               String connectionName,
                                                               String sourceObject,
                                                               String destinationObject,
                                                               String integrationApiName,
                                                               String integrationPointApiName) {

        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);

        List<String> successfulIdList = VaultCollections.newList();
        List<String> failedIdList = VaultCollections.newList();
        Boolean intRuleExists = false;

        // Instantiate the Integration Rule Service
        IntegrationRuleService integrationRuleService
                = ServiceLocator.locate(IntegrationRuleService.class);

        // Get integration rules for this connection
        GetIntegrationRulesRequest getIntegrationRulesRequest
                = integrationRuleService.newGetIntegrationRulesRequestBuilder()
                .withConnectionId(connectionId).build();
        GetIntegrationRulesResponse getIntegrationRulesResponse
                = integrationRuleService.getIntegrationRules(getIntegrationRulesRequest);

        if (!getIntegrationRulesResponse.hasError()){
            Collection<IntegrationRule> intRules
                    = (getIntegrationRulesResponse.getIntegrationRules());
            for (Iterator<IntegrationRule> intRulesiterator = intRules.iterator();
                 intRulesiterator.hasNext();) {
                IntegrationRule intRuleResult = intRulesiterator.next();
                String intRulePointName = intRuleResult.getIntegrationPoint();

                // Process the rule if the rule exists with a status of active.
                if (intRulePointName.equals(integrationPointApiName)) {

                    intRuleExists = true;
                    String queryObject
                            = intRuleResult.getFieldRules().iterator().next().getQueryObject();
                    Collection<String> queryFields = intRuleResult.getQueryFields(queryObject);

                    // Create a query from the query object/fields for the configured
                    // Integration Rule, i.e. "warranty_int_rule_1__c"
                    StringBuilder query = new StringBuilder();
                    query.append("SELECT ");
                    query.append(String.join(",", queryFields));
                    query.append(", (SELECT name__v, period_in_months__c"); // Manually add an custom fields not
                                                                            // included in the integration rules
                    query.append(" FROM warranty_period__cr)");
                    query.append(" FROM ").append(queryObject);
                    query.append(" WHERE id CONTAINS ('");
                    query.append(String.join("','", recordsToETL.keySet())).append("')");

                    // Alternatively queries can be created manually
                    // e.g.:
                    //		query.append("SELECT id, ");
                    //		query.append("name__v, ");
                    //		query.append("customer_name__c, ");
                    //		query.append("cover_start_date__c, ");
                    //		query.append("extended_cover__c, ");
                    //		query.append("manufacturer__c, ");
                    //		query.append("(SELECT name__v, period_in_months__c FROM warranty_period__cr) ");
                    //		query.append("FROM vsdk_warranty__c WHERE id CONTAINS ('" + String.join("','", recordsToCheck.keySet()) + "')";

                    logService.info("Callback query: " + query.toString());

                    //This is a vault to vault Http Request to the connection i.e. `vsdk_connection_to_warranties`
                    HttpService httpService = ServiceLocator.locate(HttpService.class);
                    HttpRequest request = httpService.newHttpRequest(connectionName);

                    //The configured connection provides the full DNS name.
                    //For the path, you only need to append the API endpoint after the DNS.
                    //The query endpoint takes a POST where the BODY is the query itself.
                    request.setMethod(HttpMethod.POST);
                    request.appendPath("/api/v20.1/query");
                    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
                    request.setHeader("X-VaultAPI-DescribeQuery", "true"); // *** New
                    request.setBodyParam("q", query.toString());

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
                                        logService.info("HTTP Query Request: SUCCESS");

                                        JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                                        JsonObject queryDescribe = response.getJsonObject().getValue("queryDescribe",
                                                                                                      JsonValueType.OBJECT);

                                        //Retrieve each record returned from the VQL query.
                                        //Each element of the returned `data` JsonArray is a record with it's queried fields.
                                        for (int i = 0; i < data.getSize();i++) {
                                            Boolean fieldRuleErrorOccurred = false;

                                            JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);
                                            String sourceId = queryRecord.getValue("id", JsonValueType.STRING);

                                            // Get the resulting values for a row based on the evaluation of the rule against a json query response record
                                            logService.info("Evaluate field rules request for rule: " + intRuleResult.getName());
                                            EvaluateFieldRulesRequest evaluateFieldRulesRequest
                                                    = integrationRuleService.newEvaluateFieldRulesRequestBuilder()
                                                    .withIntegrationRule(intRuleResult)
                                                    .withTargetName(destinationObject)
                                                    .withQueryDescribe(queryDescribe)
                                                    .withQueryData(queryRecord)
                                                    .build();
                                            EvaluateFieldRulesResponse evaluateFieldRulesResponse
                                                    = integrationRuleService.evaluateFieldRules(evaluateFieldRulesRequest);
                                            Collection<FieldRuleResult> fieldRuleResults
                                                    = evaluateFieldRulesResponse.getFieldRuleResults();

                                            // Set the records field values in the target vault using values in "fieldRuleResults"
                                            for (Iterator<FieldRuleResult> fieldRuleResultIterator
                                                 = fieldRuleResults.iterator(); fieldRuleResultIterator.hasNext(); ) {
                                                FieldRuleResult fieldRuleResult = fieldRuleResultIterator.next();
                                                if (!fieldRuleResult.hasError()) {
                                                    recordsToETL.get(sourceId).setValue(
                                                            fieldRuleResult.getTargetField(), fieldRuleResult.getValue());

                                                } else {
                                                    FieldRuleError error = fieldRuleResult.getError();

                                                    vSDKSparkHelper.createUserExceptionMessage(
                                                            integrationApiName,
                                                            integrationPointApiName,
                                                            error.getMessage(),
                                                            "vSDKIntRulesETLWarranties",
                                                            sourceId);

                                                    // Do handling based on error type and message
                                                    ItemErrorType errorType = error.getType();
                                                    String errorMessage = error.getMessage();
                                                    logService.info("Record field error type: " + errorType + "," +
                                                            " message: " + errorMessage);

                                                    // Remove the failed record so it's not processed until the field
                                                    // error is resolved
                                                    recordsToETL.remove(sourceId);

                                                    // Log the failed to update records
                                                    failedIdList.add(sourceId);
                                                    fieldRuleErrorOccurred = true;
                                                    break;
                                                }
                                            }

                                            if (!fieldRuleErrorOccurred) {

                                                //
                                                // Set any additional custom fields
                                                //

                                                // If desired query results can be used to calculate custom fields.
                                                // For instance the period_in_months__c field within the warranty_period__cr subquery
                                                // can be retrieved via a JsonObject then JsonArray.
                                                // This is basically an embedded JSON response.

                                                JsonObject subquery = queryRecord.getValue("warranty_period__cr",
                                                        JsonValueType.OBJECT);
                                                JsonArray subqueryData = subquery.getValue("data", JsonValueType.ARRAY);
                                                JsonObject subqueryRecord = subqueryData.getValue(0, JsonValueType.OBJECT);
                                                Integer warrantyPeriodMonths = Integer.parseInt(subqueryRecord.getValue(
                                                        "period_in_months__c", JsonValueType.STRING));

                                                // Now calculate the end_date__c by adding the warranty period minus a
                                                // day to the start_date__c
                                                LocalDate startDate = recordsToETL.get(sourceId).getValue(
                                                        "start_date__c", ValueType.DATE);
                                                recordsToETL.get(sourceId).setValue("end_date__c",
                                                        startDate.plusMonths(warrantyPeriodMonths).minusDays(1));

                                                // List values can also be populated, such as the integration status in
                                                // this example.
                                                recordsToETL.get(sourceId).setValue("integration_status__c",
                                                        VaultCollections.asList("processing_successful__c"));

                                                // Log the successfully updated records
                                                successfulIdList.add(sourceId);
                                            }
                                        }

                                        data = null;
                                    }
                                    else {
                                        for (String key : recordsToETL.keySet()) {
                                            failedIdList.add(key);
                                        }
                                    }
                                    response = null;
                                }

                            })
                            .onError(httpOperationError -> {
                                for (String key : recordsToETL.keySet()) {
                                    failedIdList.add(key);
                                }
                            }).execute();

                    // Save the transformed records using a batchSaveRecords
                    List<Record> transformedRecordList = VaultCollections.newList();
                    if (recordsToETL.size() > 0) {
                        transformedRecordList.addAll(recordsToETL.values());
                        recordsToETL.clear();

                        recordService.batchSaveRecords(transformedRecordList).onSuccesses(successMessage -> {

                            List<Record> successfulRecords = VaultCollections.newList();
                            successMessage.stream().forEach(positionalRecordId -> {
                                Record record = recordService.newRecordWithId(destinationObject,
                                        positionalRecordId.getRecordId());
                                successfulRecords.add(record);
                            });

                        }).onErrors(batchOperationErrors -> {
                            batchOperationErrors.stream().forEach(error -> {
                                String errMsg = error.getError().getMessage();
                                int errPosition = error.getInputPosition();
                                String sourceId = transformedRecordList.get(errPosition).getValue("source_record__c", ValueType.STRING);

                                // Create a User Exception per record error, as each one may be different
                                //
                                // An example of when this error might occur would be if a mandatory field in the
                                // target object wasn't populated.
                                StringBuilder logMessage = new StringBuilder();
                                logMessage.append("Unable to create '").append(transformedRecordList.get(errPosition).getObjectName())
                                        .append("' record, because of '").append(errMsg).append("'.");

                                vSDKSparkHelper.createUserExceptionMessage(
                                        integrationApiName,
                                        integrationPointApiName,
                                        logMessage.toString(),
                                        "vSDKIntRulesETLWarranties",
                                        sourceId);

                                // Move the failed record from the successful to failed list
                                successfulIdList.remove(sourceId);
                                failedIdList.add(sourceId);

                            });
                        }).execute();

                    }

                    // Update the source system to say whether records have updated successfully or not,
                    // by making a callback to the source vault and updating the integration transaction
                    // record associated with the source record.
                    vSDKSparkHelper.setIntTransProcessedStatuses(
                            successfulIdList,
                            connectionName, // i.e. vsdk_connection_to_warranties
                            queryObject, // i.e. vsdk_warranty__c
                            integrationPointApiName,
                            true);

                    vSDKSparkHelper.setIntTransProcessedStatuses(
                            failedIdList,
                            connectionName, // i.e. vsdk_connection_to_warranties
                            queryObject, // i.e. vsdk_warranty__c
                            integrationPointApiName,
                            false);

                    request = null;
                    break;

                }
                intRules = null;
            }

            // Handle integration rule not existing here
            if (!intRuleExists) {
                String unprocessedRecordKeys = String.join(",", recordsToETL.keySet());
                vSDKSparkHelper.createUserExceptionMessage(
                        integrationApiName,
                        integrationPointApiName,
                        "There is no active integration rule for this integration point",
                        "vSDKIntRulesETLWarranties",
                        unprocessedRecordKeys);
            }

        //
        // Do handling IntegrationRulesResponse Errors based on error type and message
        } else {
            IntegrationRuleError error = getIntegrationRulesResponse.getError();
            MessageErrorType errorType = error.getType();

            // In this case, a single user exception is created for all of the failed records
            String unprocessedRecordKeys = String.join(",", recordsToETL.keySet());
            vSDKSparkHelper.createUserExceptionMessage(
                    integrationApiName,
                    integrationPointApiName,
                    error.getMessage(),
                    "vSDKIntRulesETLWarranties",
                    unprocessedRecordKeys);

            // If desired the source records could be marked as failed as shown below, however this
            // may not be the best course of action, since if the error is config related, once fixed
            // these records may need to be reprocessed.
            // e.g.:
            //   for (String key : recordsToETL.keySet()) {
            //       failedIdList.add(key);
            //   }
            //   vSDKSparkHelper.setIntTransProcessedStatuses(
            //           failedIdList,
            //           connectionName,
            //           sourceObject,
            //           integrationPointApiName,
            //           false);

        }
    }

}
