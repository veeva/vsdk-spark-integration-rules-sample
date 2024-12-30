package com.veeva.vault.custom.udc;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordBatchSaveRequest;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.http.*;
import com.veeva.vault.sdk.api.json.*;
import com.veeva.vault.sdk.api.query.*;
import com.veeva.vault.sdk.api.queue.Message;
import com.veeva.vault.sdk.api.queue.PutMessageResponse;
import com.veeva.vault.sdk.api.queue.PutMessageResult;
import com.veeva.vault.sdk.api.queue.QueueService;

import java.util.List;

/******************************************************************************
 * User-Defined Class:  vSDKSparkHelper
 * Author:              John Tanner @ Veeva
 * Date:                2020-02-03
 *-----------------------------------------------------------------------------
 * Description: Useful Methods to help with Spark Integration between Vaults
 *
 *-----------------------------------------------------------------------------
 * Copyright (c) 2023 Veeva Systems Inc.  All Rights Reserved.
 *
 * This code is based on pre-existing content developed and
 * owned by Veeva Systems Inc. and may only be used in connection
 * with the deliverable with which it was provided to Customer.
 *--------------------------------------------------------------------
 *
 *******************************************************************************/

@UserDefinedClassInfo()
public class vSDKSparkHelper {

    /**
     * Returns whether an active version of the specified 'integrationName'
     * is configured against this (source) Vault.
     *
     * @param integrationName
     */
    public static boolean isIntegrationActive(String integrationName) {
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        LogService logService = ServiceLocator.locate(LogService.class);
        List<Integer> recordCount = VaultCollections.newList();

        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id", "name__v", "status__v"))
                .withFrom("integration__sys")
                .withWhere("name__v = '" + integrationName + "'")
                .appendWhere(QueryLogicalOperator.AND, "status__v = 'active__v'")
                .build();

        QueryCountRequest queryCountRequest  = queryService
                .newQueryCountRequestBuilder()
                .withQuery(query)
                .build();
        queryService.count(queryCountRequest)
                .onSuccess(queryResponse -> {
                    recordCount.add((int)queryResponse.getTotalCount());
                })
                .onError(queryOperationError -> {
                    logService.error("Failed to query records: " + queryOperationError.getMessage());
                })
                .execute();

        if (recordCount.get(0) == 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Creates a User exception message
     *
     * @param integrationApiName
     * @param integrationPointApiName
     * @param errorMessage
     * @param messageProcessor
     * @param messageBody stores the message identifier so it can be re-run
     */
    public static void createUserExceptionMessage(String integrationApiName,
                                                  String integrationPointApiName,
                                                  String errorMessage,
                                                  String messageProcessor,
                                                  String messageBody) {

        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);
        String integrationId = getIntegrationId(integrationApiName);
        String integrationPointId = getIntegrationPointId(integrationPointApiName);

        // Construct the user exception message
        Record userExceptionMessage = recordService.newRecord("exception_message__sys");
        List<String> errorTypePicklistValues = VaultCollections.newList();
        errorTypePicklistValues.add("message_processing_error__sys");
        userExceptionMessage.setValue("integration__sys", integrationId);
        userExceptionMessage.setValue("integration_point__sys", integrationPointId);
        userExceptionMessage.setValue("error_type__sys", errorTypePicklistValues);
        userExceptionMessage.setValue("error_message__sys", errorMessage);
        userExceptionMessage.setValue("name__v", integrationPointApiName);
        userExceptionMessage.setValue("message_processor__sys", messageProcessor);
        userExceptionMessage.setValue("message_body_json__sys", messageBody);
        List<Record> recordsToSave = VaultCollections.newList();
        recordsToSave.add(userExceptionMessage);

        // Save the User Exception
        if (!recordsToSave.isEmpty()) {
            RecordBatchSaveRequest recordBatchSaveRequest = recordService.newRecordBatchSaveRequestBuilder()
                    .withRecords(recordsToSave)
                    .build();

            recordService.batchSaveRecords(recordBatchSaveRequest)
                    .onErrors(batchOperationErrors -> {
                        batchOperationErrors.stream().findFirst().ifPresent(error -> {
                            String errMsg = error.getError().getMessage();
                            int errPosition = error.getInputPosition();
                            String name = recordsToSave.get(errPosition).getValue("name__v", ValueType.STRING);
                            logService.error("Unable to create '" + recordsToSave.get(errPosition).getObjectName() + "' record: '" +
                                    name + "' because of '" + errMsg + "'.");
                        });
                    })
                    .rollbackOnErrors()
                    .execute();
        }

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Created User exception message: ").append(errorMessage);
        logService.info(logMessage.toString());
    }

    /**
     * Returns the ID of the specified 'integrationName'
     *
     * @param integrationAPIName of the integration
     */
    public static String getIntegrationId(String integrationAPIName) {
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        LogService logService = ServiceLocator.locate(LogService.class);
        List<String> ids = VaultCollections.newList();

        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("integration__sys")
                .withWhere("integration_api_name__sys = '" + integrationAPIName + "'")
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                .withQuery(query)
                .build();

        QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

        queryOperation.onSuccess(queryExecutionResponse -> {
            queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                ids.add(queryExecutionResult.getValue("id", ValueType.STRING));
            });
        });
        queryOperation.onError(queryOperationError -> {
            logService.error("Failed to query records: " + queryOperationError.getMessage());
        });
        queryOperation.execute();

        return ids.get(0);
    }

    /**
     * Returns the ID of the specified 'integrationPointAPIName'
     *
     * @param integrationPointAPIName of the integration
     */
    public static String getIntegrationPointId(String integrationPointAPIName) {
        LogService logService = ServiceLocator.locate(LogService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        List<String> ids = VaultCollections.newList();

        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("integration_point__sys")
                .withWhere("integration_point_api_name__sys = '" + integrationPointAPIName + "'")
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                .withQuery(query)
                .build();

        QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

        queryOperation.onSuccess(queryExecutionResponse -> {
            queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                ids.add(queryExecutionResult.getValue("id", ValueType.STRING));
            });
        });
        queryOperation.onError(queryOperationError -> {
            logService.error("Failed to query records: " + queryOperationError.getMessage());
        });
        queryOperation.execute();

        return ids.get(0);
    }

    /**
     * Returns the ID of the specified 'connectionName'
     *
     * @param connectionAPIName of the integration
     */
    public static String getConnectionId(String connectionAPIName) {
        LogService logService = ServiceLocator.locate(LogService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        List<String> ids = VaultCollections.newList();

        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("connection__sys")
                .withWhere("api_name__sys = '" + connectionAPIName + "'")
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                .withQuery(query)
                .build();

        QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

        queryOperation.onSuccess(queryExecutionResponse -> {
            queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                ids.add(queryExecutionResult.getValue("id", ValueType.STRING));
            });
        });
        queryOperation.onError(queryOperationError -> {
            logService.error("Failed to query records: " + queryOperationError.getMessage());
        });
        queryOperation.execute();

        return ids.get(0);
    }

    /**
     *  Return a list of objects which haven't been migrated
     *  This uses a callback to the source vault to check for records in the integration_transaction__c object
     *  for the object/integration point, which have a processed status of pending and a last modified date
     *  greater than 10 seconds before. This wait is to allow for the job already being processed on a separate thread.
     *
     * @param connectionName to the source Vault
     * @param objectName of the object name to be migrated from the source Vault
     * @param targetIntegrationPoint for the source record
     * @param runImmediately if true it doesn't wait for the 10 seconds before processing
     */
    public static List<String> getUnprocessedObjects(String connectionName, // e.g. vsdk_connection_to_warranties
                                                     String objectName, // e.g. vsdk_warranty__c
                                                     String targetIntegrationPoint, // e.g. send_to_claims_pending__c
                                                     Boolean runImmediately) {

        String reprocessBeforeDate = java.time.Clock.systemUTC().instant().minusSeconds(10).toString();

        LogService logService = ServiceLocator.locate(LogService.class);
        List<String> unprocessedObjectsList = VaultCollections.newList();
        StringBuilder query = new StringBuilder();
        query.append("SELECT source_key__c ");
        query.append("FROM integration_transaction__c ");
        query.append("WHERE transaction_status__c = 'pending__c' ");
        query.append("AND source_object__c = '").append(objectName).append("' ");
        query.append("AND target_integration_point__c = '").append(targetIntegrationPoint).append("' ");
        if (!runImmediately) {
            query.append("AND modified_date__v < '").append(reprocessBeforeDate).append("' ");
        }


        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Unprocessed Callback query: ").append(query.toString());
        logService.info(logMessage.toString());

        //This is a vault to vault Http Request to the source connection
        //The configured connection provides the full DNS name.
        //For the path, you only need to append the API endpoint after the DNS.
        //The query endpoint takes a POST where the BODY is the query itself.
        HttpService httpService = ServiceLocator.locate(HttpService.class);
        FormHttpRequest request = httpService.newHttpRequestBuilder()
                .withConnectionName(connectionName)
                .withMethod(HttpMethod.POST)
                .withPath("/api/v24.2/query")
                .withHeader("Content-Type", "application/x-www-form-urlencoded")
                .withBodyParam("q", query.toString())
                .build();

        //Send the request to the source vault via a callback. The response received back should be a JSON response.
        //First, the response is parsed into a `JsonData` object
        //From the response, the `getJsonObject()` will get the response as a parseable `JsonObject`
        //    * Here the `getValue` method can be used to retrieve `responseStatus`, `responseDetails`, and `data`
        //The `data` element is an array of JSON data. This is parsed into a `JsonArray` object.
        //    * Each queried record is returned as an element of the array and must be parsed into a `JsonObject`.
        //    * Individual fields can then be retrieved from each `JsonObject` that is in the `JsonArray`.
        httpService.sendRequest(request, HttpResponseBodyValueType.JSONDATA)
                .onSuccess(httpResponse -> {

                    JsonData response = httpResponse.getResponseBody();

                    if (response.isValidJson()) {
                        String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                        if (responseStatus.equals("SUCCESS")) {

                            JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                            logService.info("HTTP Query Request: SUCCESS");

                            //Retrieve each record returned from the VQL query.
                            //Each element of the returned `data` JsonArray is a record with it's queried fields.
                            for (int i = 0; i < data.getSize();i++) {
                                JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);
                                String sourceId = queryRecord.getValue("source_key__c", JsonValueType.STRING);
                                unprocessedObjectsList.add(sourceId);
                            }
                            data = null;
                        }
                        response = null;
                    }
                    else {
                        logService.info("getUnprocessedObjects error: Received a non-JSON response.");
                    }
                })
                .onError(httpOperationError -> {
                    logService.info("getUnprocessedObjects error: httpOperationError.");
                }).execute();

        request = null;
        return unprocessedObjectsList;

    }

    /**
     *  Return a list of Integration Transaction Record Ids, associated with the given
     *  composite key which consists of source_record_id, source_object, target_integration_point
     *  and processed_status of pending
     *
     * @param connectionName to the source Vault
     * @param objectName of the object name to be migrated from the source Vault
     * @param targetIntegrationPoint for the source record
     */
    public static List<String> getIntTransIds(String connectionName, // i.e. vsdk_connection_to_warranties
                                              List<String> sourceRecordIds,
                                              String objectName, // i.e. vsdk_warranty__c
                                              String targetIntegrationPoint) { // i.e. send_to_claims_pending__c

        LogService logService = ServiceLocator.locate(LogService.class);
        List<String> idList = VaultCollections.newList();
        StringBuilder query = new StringBuilder();
        query.append("SELECT id ");
        query.append("FROM integration_transaction__c ");
        query.append("WHERE transaction_status__c = 'pending__c' ");
        query.append("AND source_object__c = '").append(objectName).append("' ");
        query.append("AND source_key__c contains ('").append(String.join("','", sourceRecordIds)).append("') ");
        query.append("AND target_integration_point__c = '").append(targetIntegrationPoint).append("' ");


        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Int Transactions Ids Callback query: ").append(query.toString());
        logService.info(logMessage.toString());

        //This is a vault to vault Http Request to the source connection
        //The configured connection provides the full DNS name.
        //For the path, you only need to append the API endpoint after the DNS.
        //The query endpoint takes a POST where the BODY is the query itself.
        HttpService httpService = ServiceLocator.locate(HttpService.class);
        FormHttpRequest request = httpService.newHttpRequestBuilder()
                .withConnectionName(connectionName)
                .withMethod(HttpMethod.POST)
                .withPath("/api/v24.2/query")
                .withHeader("Content-Type", "application/x-www-form-urlencoded")
                .withBodyParam("q", query.toString())
                .build();

        httpService.sendRequest(request, HttpResponseBodyValueType.JSONDATA)
                .onSuccess(httpResponse -> {

                    JsonData response = httpResponse.getResponseBody();

                    if (response.isValidJson()) {
                        String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                        if (responseStatus.equals("SUCCESS")) {

                            JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                            logService.info("HTTP Query Request: SUCCESS");

                            //Retrieve each record returned from the VQL query.
                            //Each element of the returned `data` JsonArray is a record with it's queried fields.
                            for (int i = 0; i < data.getSize();i++) {
                                JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);
                                String sourceId = queryRecord.getValue("id", JsonValueType.STRING);
                                idList.add(sourceId);
                            }
                            data = null;
                        }
                        response = null;
                    }
                    else {
                        logService.info("getIntTransIds error: Received a non-JSON response.");
                    }
                })
                .onError(httpOperationError -> {
                    logService.info("getIntTransIds error: httpOperationError.");
                }).execute();

        request = null;
        return idList;
    }

    /**
     *  Sets the status of the source objects that have been migrated
     *  This uses a callback to the source vault for the object, based on the unprocessed status field/value
     *
     * @param recordsUpdateSourceRecordIds is a list parameter containing the affected source record Ids
     * @param connectionName to the source Vault
     * @param objectName of the object name to be migrated from the source Vault
     * @param targetIntegrationPoint for the source record
     * @param processSuccess for the source record
     */
    //The recordsUpdate list parameter contains the affected source Ids.
    //The source Ids are used to build a batch of vsdk_bike_store__c updates for the source system.
    //The Update Record API takes JSON data as the body of the HttpRequest, so we can
    //  build a Json request with the `JsonObjectBuilder` and `JsonArrayBuilder`
    public static void setIntTransProcessedStatuses(List<String> recordsUpdateSourceRecordIds,
                                                    String connectionName, // i.e. vsdk_connection_to_warranties
                                                    String objectName, // i.e. vsdk_warranty__c
                                                    String targetIntegrationPoint,
                                                    Boolean processSuccess) { // i.e. receive_warranties__c

        HttpService httpService = ServiceLocator.locate(HttpService.class);
        LogService logService = ServiceLocator.locate(LogService.class);

        String processValue = "process_success__c";
        if (!processSuccess) {
            processValue = "process_failure__c";
        }

        // Get a list of Integration Transaction Record Ids, associated with the given
        // composite key which consists of source_record_id, source_object, target_integration_point
        // and processed_status of pending
        List<String> recordUpdateIds = getIntTransIds(connectionName,
                recordsUpdateSourceRecordIds,
                objectName,
                targetIntegrationPoint);

        FormHttpRequest.Builder formBuilder = httpService.newHttpRequestBuilder()
                .withConnectionName(connectionName);

        JsonService jsonService = ServiceLocator.locate(JsonService.class);
        JsonObjectBuilder jsonObjectBuilder = jsonService.newJsonObjectBuilder();
        JsonArrayBuilder jsonArrayBuilder = jsonService.newJsonArrayBuilder();

        //The Update Object Record API takes JSON data as input.
        //The input format is an array of Json objects.
        //Use the `JsonObjectBuilder` to build the individual Json Objects with the necessary updates.
        //Then add the resulting `JsonObject` objects to the `JsonArrayBuilder`
        for (String value : recordUpdateIds) {

            JsonObject inputJsonObject = jsonObjectBuilder.setValue("id", value)
                    .setValue("transaction_status__c", processValue)
                    .build();
            jsonArrayBuilder.add(inputJsonObject);
        }

        //Once all the Json objects are added to the `JsonArray`, use the `build` method to generate the array.
        JsonArray inputJsonArray = jsonArrayBuilder.build();

        if (inputJsonArray.getSize() > 0) {
            FormHttpRequest request = formBuilder
                    .withMethod(HttpMethod.PUT)
                    .withPath("/api/v24.2/vobjects/integration_transaction__c")
                    .withHeader("Content-Type", "application/json")
                    .withBody(inputJsonArray)
                    .build();

            httpService.sendRequest(request, HttpResponseBodyValueType.JSONDATA)
                    .onSuccess(httpResponse -> {

                        JsonData response = httpResponse.getResponseBody();

                        if (response.isValidJson()) {
                            String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                            if (responseStatus.equals("SUCCESS")) {
                                JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);

                                //Retrieve the results for each record that was updated.
                                //Each element of the returned `data` JsonArray is the results of a single updated record.
                                for (int i = 0; i <= data.getSize()-1;i++) {
                                    JsonObject recordResponse = data.getValue(i, JsonValueType.OBJECT);
                                    String recordStatus = recordResponse.getValue("responseStatus", JsonValueType.STRING);

                                    JsonObject recordData = recordResponse.getValue("data", JsonValueType.OBJECT);
                                    String recordId = recordData.getValue("id", JsonValueType.STRING);

                                    StringBuilder logMessage = new StringBuilder();
                                    logMessage.append("HTTP Update Request ").append(recordStatus)
                                            .append(": ").append(recordId);
                                    logService.info(logMessage.toString());
                                }
                                data = null;
                            }
                            response = null;
                        }
                        else {
                            logService.info("v2vHttpUpdate error: Received a non-JSON response.");
                        }
                    })
                    .onError(httpOperationError -> {
                        logService.info(httpOperationError.getMessage());
                        logService.info(httpOperationError.getHttpResponse().getResponseBody());
                    }).execute();

            request = null;
            inputJsonArray = null;
        }
    }

    // Move to the Spark Queue AFTER the record has successfully been inserted.
    public static void moveMessagesToQueue(String queueName, String objectName, String targetIntegrationPointAPIName, String recordEvent, List vaultIds) {

        QueueService queueService = ServiceLocator.locate(QueueService.class);
        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);

        Message message = queueService.newMessage(queueName)
                .setAttribute("object", objectName)
                .setAttribute("event", recordEvent)
                .setAttribute("integration_point", targetIntegrationPointAPIName);
        PutMessageResponse response = queueService.putMessage(message);

        // Check that the message queue successfully processed the message.
        // If it's successful, create an integrationTransaction record setting the `transaction_status__c` flag to 'pending__c',
        // as long as it doesn't already exist, as there is no point duplicating it.
        // Otherwise if there is an error, create an integrationTransaction record setting the `transaction_status__c` flag to 'send_failure__c'.
        List<Record> intTransactionsToSave = VaultCollections.newList();
        List<PutMessageResult> messageResultList = response.getPutMessageResults();
        for (PutMessageResult messageResult : messageResultList) {
            logService.info("Sent to Connection: " + messageResult.getConnectionName());

            String connectionId = vSDKSparkHelper.getConnectionId(messageResult.getConnectionName());
            logService.info("connectionId: " + connectionId);
            logService.info("integrationPointAPIName: " + targetIntegrationPointAPIName);

            Query query = queryService
                    .newQueryBuilder()
                    .withSelect(VaultCollections.asList("source_key__c"))
                    .withFrom("integration_transaction__c")
                    .withWhere("source_key__c contains ('" + String.join("','", vaultIds) + "') ")
                    .appendWhere(QueryLogicalOperator.AND, "source_object__c = '" + objectName + "' ")
                    .appendWhere(QueryLogicalOperator.AND, "connection__c = '" + connectionId + "' ")
                    .appendWhere(QueryLogicalOperator.AND, "target_integration_point__c = '" + targetIntegrationPointAPIName + "' ")
                    .appendWhere(QueryLogicalOperator.AND, "transaction_status__c = 'pending__c' ")
                    .build();

            QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                    .withQuery(query)
                    .build();

            QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

            logService.info("Query existing pending integration transactions by ID: " + query);

            // Any pending integration transactions records that already exist will be removed from the list
            // so they don't get recreated
            queryOperation.onSuccess(queryExecutionResponse -> {
                queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                    String source_key__c = queryExecutionResult.getValue("source_key__c", ValueType.STRING);
                    logService.info("Found existing pending record with Source Key: " + source_key__c);
                    vaultIds.remove(source_key__c);
                });
            });
            queryOperation.onError(queryOperationError -> {
                logService.error("Failed to query records: " + queryOperationError.getMessage());
            });
            queryOperation.execute();

            for (Object vaultId : vaultIds) {

                Record integrationTransaction = recordService.newRecord("integration_transaction__c");
                integrationTransaction.setValue("source_object__c", objectName);
                integrationTransaction.setValue("source_key__c", vaultId);
                integrationTransaction.setValue("target_integration_point__c", targetIntegrationPointAPIName);
                integrationTransaction.setValue("connection__c", connectionId);
                if (response.getError() != null) {
                    integrationTransaction.setValue("transaction_status__c", VaultCollections.asList("send_failure__c"));
                    StringBuilder err = new StringBuilder();
                    err.append("ERROR Queuing Failed: ").append(response.getError().getMessage());
                    integrationTransaction.setValue("message__c", err.toString().substring(1,1500));
                } else {
                    integrationTransaction.setValue("transaction_status__c", VaultCollections.asList("pending__c"));
                }
                StringBuilder recordName = new StringBuilder();
                recordName.append(recordEvent).append(" ")
                        .append(vaultId)
                        .append(" to ")
                        .append(targetIntegrationPointAPIName);
                integrationTransaction.setValue("name__v", recordName.toString());
                intTransactionsToSave.add(integrationTransaction);
            }
            logService.info("Completed connection: " + messageResult.getConnectionName());
        }

        // Save the Integration Transactions for the connection
        if (intTransactionsToSave.size() > 0) {
            RecordBatchSaveRequest recordBatchSaveRequest = recordService.newRecordBatchSaveRequestBuilder()
                    .withRecords(intTransactionsToSave)
                    .build();

            recordService.batchSaveRecords(recordBatchSaveRequest)
                    .onErrors(batchOperationErrors -> {
                        //Iterate over the caught errors.
                        //The BatchOperation.onErrors() returns a list of BatchOperationErrors.
                        //The list can then be traversed to retrieve a single BatchOperationError and
                        //then extract an **ErrorResult** with BatchOperationError.getError().
                        batchOperationErrors.stream().findFirst().ifPresent(error -> {
                            String errMsg = error.getError().getMessage();
                            int errPosition = error.getInputPosition();
                            StringBuilder err = new StringBuilder();
                            String name = intTransactionsToSave.get(errPosition).getValue("source_key__c", ValueType.STRING);
                            err.append("Unable to create '")
                                    .append(intTransactionsToSave.get(errPosition).getObjectName())
                                    .append("' record: '")
                                    .append(name)
                                    .append("' because of '")
                                    .append(errMsg)
                                    .append("'.");
                            throw new RollbackException("OPERATION_NOT_ALLOWED", err.toString());
                        });
                    })
                    .execute();

            StringBuilder logMessage = new StringBuilder();
            logMessage.append("Created Integration Transaction for integration point : ").append(targetIntegrationPointAPIName).append(".");
            logService.info(logMessage.toString());
        }

    }
}
