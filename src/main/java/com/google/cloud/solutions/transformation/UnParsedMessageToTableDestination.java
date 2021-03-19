package com.google.cloud.solutions.transformation;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;
import com.google.cloud.solutions.utils.TableDestinationLoader;
import com.google.cloud.solutions.utils.TableSchemaLoader;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class UnParsedMessageToTableDestination extends DynamicDestinations<UnParsedMessage, IoTCoreMessageInfo> {

    private static final long serialVersionUID = 1L;
    private static final String MESSAGE_TYPE = "unknown-message";

    @Override
    public IoTCoreMessageInfo getDestination(ValueInSingleWindow<UnParsedMessage> element) {
        return element.getValue().getMessageInfo();
    }

    @Override
    public TableDestination getTable(IoTCoreMessageInfo messageInfo) {
        messageInfo.setMessageType(MESSAGE_TYPE);
        return TableDestinationLoader.getDestination(messageInfo);
    }

    @Override
    public TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
        messageInfo.setMessageType(MESSAGE_TYPE);
        return TableSchemaLoader.getSchema(messageInfo);
    }

}