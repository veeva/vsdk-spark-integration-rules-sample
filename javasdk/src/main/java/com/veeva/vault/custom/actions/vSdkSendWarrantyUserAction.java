package com.veeva.vault.custom.actions;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.action.RecordAction;
import com.veeva.vault.sdk.api.action.RecordActionContext;
import com.veeva.vault.sdk.api.action.RecordActionInfo;

import com.veeva.vault.sdk.api.data.Record;
import com.veeva.vault.custom.udc.vSDKSparkHelper;

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
        String integrationName = "Warranties";
        String targetIntegrationPointAPIName = "receive_warranty__c";

        if (vSDKSparkHelper.isIntegrationActive(integrationName)) {
            logService.info("Integration '" + integrationName + "' is active.");
            for (Record actionRecord : recordActionContext.getRecords()) {

                if (vaultIds.size() < 500) {
                    vaultIds.add(actionRecord.getValue("id", ValueType.STRING));
                } else {
                    vSDKSparkHelper.moveMessagesToQueue("vsdk_warranty_out_queue__c",
                            "vsdk_warranty__c",
                            targetIntegrationPointAPIName,
                            event,
                            vaultIds);
                    vaultIds.clear();
                }
            }

            if (vaultIds.size() > 0) {
                vSDKSparkHelper.moveMessagesToQueue("vsdk_warranty_out_queue__c",
                        "vsdk_warranty__c",
                        targetIntegrationPointAPIName,
                        event,
                        vaultIds);
            }

        } else {

            // Don't send message and handle as desired.
            // e.g. Return a message to the user is the integration isn't active
            throw new RollbackException("INTEGRATION_WARNING", "Cannot send this " +
                    "record as no active version of the integration '" +
                    integrationName + "' is configured.");
        }
    }
}