package com.veeva.vault.custom.actions;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.action.RecordAction;
import com.veeva.vault.sdk.api.action.RecordActionContext;
import com.veeva.vault.sdk.api.action.RecordActionInfo;

import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.sdk.api.data.RecordService;
import com.veeva.vault.sdk.api.queue.Message;
import com.veeva.vault.sdk.api.queue.PutMessageResponse;
import com.veeva.vault.sdk.api.queue.QueueService;
import java.util.List;

/**
 * vSDK SPARK Send Warranty Record Action
 *
 * This record action is pushes a warranty record created in Vault to an
 * second claims Vault for processing via a message processor
 */
@RecordActionInfo(label="Send Warranty", object="vsdk_warranty__c")
public class vSdkSendWarrantyUserAction implements RecordAction {
    // This action is available for configuration in Vault Admin.
    public boolean isExecutable(RecordActionContext context) {
        return true;
    }
    public void execute(RecordActionContext recordActionContext) {

        LogService logService = ServiceLocator.locate(LogService.class);

        List vaultIds = VaultCollections.newList();
        String event = "Send Warranty";

        for (Record actionRecord : recordActionContext.getRecords()) {

            if (vaultIds.size() < 500) {
                vaultIds.add(actionRecord.getValue("id", ValueType.STRING));
            } else {
                moveMessagesToQueue( "vsdk_warranty_out_queue__c",
                        "vsdk_warranty__c",
                        event,
                        vaultIds);
                vaultIds.clear();
            }
        }

        if (vaultIds.size() > 0){
            moveMessagesToQueue( "vsdk_warranty_out_queue__c",
                    "vsdk_warranty__c",
                    event,
                    vaultIds);
        }

    }

    // Move to the Spark Queue AFTER the record has successfully been inserted.
    public void moveMessagesToQueue(String queueName, String objectName, String recordEvent, List vaultIds) {

        QueueService queueService = ServiceLocator.locate(QueueService.class);
        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);
        List<Record> recordList = VaultCollections.newList();
        Message message = queueService.newMessage(queueName)
                .setAttribute("object", objectName)
                .setAttribute("event", recordEvent)
                .setAttribute("integration_point", "receive_warranty__c")
                .setMessageItems(vaultIds);
        PutMessageResponse response = queueService.putMessage(message);

        //Check that the message queue successfully processed the message.
        //If it's successful, change the `send_to_claims_status__c` flag to 'send_to_claims_pending__c'.
        //If there is an error, change the `send_to_claims_status__c` flag to 'send_to_claims_failed__c'.
        if (response.getError() != null) {
            logService.info("ERROR Queuing Failed: " + response.getError().getMessage());

            for (Object vaultId : vaultIds) {
                Record recordUpdate = recordService.newRecordWithId(objectName, vaultId.toString());
                recordUpdate.setValue("send_to_claims_status__c", VaultCollections.asList("send_to_claims_failed__c"));
                recordList.add(recordUpdate);
            }
        }
        else {
            for (Object vaultId : vaultIds) {
                Record recordUpdate = recordService.newRecordWithId(objectName, vaultId.toString());
                recordUpdate.setValue("send_to_claims_status__c", VaultCollections.asList("send_to_claims_pending__c"));
                recordList.add(recordUpdate);
            }
        }

        //If a subsequent error occurs save the record change, raise an 'OPERATION_NOT_ALLOWED'
        //error through the Vault UI.
        if (recordList.size() > 0) {
            recordService.batchSaveRecords(recordList)
                    .onErrors(batchOperationErrors -> {

                        //Iterate over the caught errors.
                        //The BatchOperation.onErrors() returns a list of BatchOperationErrors.
                        //The list can then be traversed to retrieve a single BatchOperationError and
                        //then extract an **ErrorResult** with BatchOperationError.getError().
                        batchOperationErrors.stream().findFirst().ifPresent(error -> {
                            String errMsg = error.getError().getMessage();
                            int errPosition = error.getInputPosition();
                            String name = recordList.get(errPosition).getValue("name__v", ValueType.STRING);
                            throw new RollbackException("OPERATION_NOT_ALLOWED", "Unable to create '" +
                                    recordList.get(errPosition).getObjectName() + "' record: '" +
                                    name + "' because of '" + errMsg + "'.");
                        });
                    })
                    .execute();
        }
    }
}