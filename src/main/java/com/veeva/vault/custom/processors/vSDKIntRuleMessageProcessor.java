package com.veeva.vault.custom.processors;

import com.veeva.vault.custom.udc.vSDKSparkHelper;
import com.veeva.vault.custom.udc.vSDKIntRulesETLWarranties;
import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.queue.*;

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

    /**
     * Main Spark v2v MessaageProcessor. It determines the Integration Point
     * related to the message and call the appropriate Processing code
     * which uses a combination of Integration Rules and SDK Logic
     * @param context is the MessageProcessor context
     */

	public void execute(MessageContext context) {

        //Processes a message from the queue which aren't blank
    	Message incomingMessage = context.getMessage();

        // Here we want to process messages specific to the different integration
        // points. These message attributes are set in the user action
        // "vSdkSendWarrantyUserAction" in the source Vault
            String integrationPointApiName =
                incomingMessage.getAttribute("integration_point",
                                                 MessageAttributeValueType.STRING);

        switch(integrationPointApiName) {
            case "receive_warranty__c":
                // Here we want to process the messages associated with the inbound
                // Integration Point "Receive Warranty". This is done in the method
                // "intRulesETLWarrantiesMessage"
                intRulesETLWarrantiesMessage(incomingMessage, context);
                break;
                // Add further case statements here if you wish to handle further
                // integration points
        }
    }

    /**
     * Processes "Receive Warranty" messages from the queue.
     *
     * When a message is received the "Integration transaction" record in the source
     * system is checked to look for any records with a status of "Pending" which
     * need to be migrated to the target vault as a "Claims Warranty" records.
     *
     * If the records already exists in the target Vault the record will be created
     * otherwise a new record will be created. To obtain source data an API VQL query
     * is made to the source vault to grab data from the source record. This queried
     * information is added to the target record using Integration rules to determine
     * source and target Vault field mappings.
     *
     * After it is updated, an API record update to sent to the source vault to
     * update the `integration_status__c`:
     *  - `successfully_processed__c` - if the query and update are successful
     *  - `processing_failed__c`       - if the query or update are unsuccessful
     *
     *  The "Integration transaction" record is also updated to "Pending" to "Success"
     *  or "Failure" so the record doesn't get remigrated.
     *
     * @param message Spark message .
     * @param context is the MessageProcessor context
     */
	public static void intRulesETLWarrantiesMessage(Message message,
                                                    MessageContext context) {

        LogService logService = ServiceLocator.locate(LogService.class);
        String sourceVaultId = context.getRemoteVaultId();
        String connectionId = context.getConnectionId();
        String integrationPointApiName =
                message.getAttribute("integration_point", MessageAttributeValueType.STRING);

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Processing integrationPointApiName: ").append(integrationPointApiName);
        logService.info(logMessage.toString());

        String connectionName = "vsdk_connection_to_warranties";
        String sourceObject = "vsdk_warranty__c";


        // Check for items which haven't been migrated
        // This uses a callback to the source vault for the object
        // then add them to the incoming message list for processing
        List<String> unprocessedList = vSDKSparkHelper.getUnprocessedObjects(
                connectionName,
                sourceObject,
                integrationPointApiName,
                true);

        vSDKIntRulesETLWarranties.process(
                unprocessedList,
                sourceVaultId,
                sourceObject,
                connectionId,
                connectionName,
                integrationPointApiName);
    }
}