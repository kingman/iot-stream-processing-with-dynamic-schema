package com.google.cloud.solutions.transformation;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.TableDestinationLoader;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

public class DynamicMessageToTableDestination extends DynamicDestinations<TableRowWithMessageInfo, IoTCoreMessageInfo> {
    private static final long serialVersionUID = -1519270901335637794L;

    @Override
    public IoTCoreMessageInfo getDestination(ValueInSingleWindow<TableRowWithMessageInfo> element) {
        return element.getValue().getMessageInfo();
    }

    @Override
    public TableDestination getTable(IoTCoreMessageInfo messageInfo) {
        return TableDestinationLoader.getDestination(messageInfo);
    }

    @Override
    public TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
        return TableSchemaLoader.getSchema(messageInfo);
    }
}
