package com.google.cloud.solutions.utils;

import java.util.HashMap;
import java.util.Map;

import com.google.api.services.bigquery.model.TableReference;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

public class TableDestinationLoader {
    private static final String DESTINATION_TABLE_METADATA_PREFIX = "destination-table-";
    private static final String DESTINATION_DATASET_METADATA_PREFIX = "destination-dataset-";
    private static final Map<String, TableDestination> destinationCache = new HashMap<>();

    public static TableDestination getDestination(IoTCoreMessageInfo messageInfo) {
        final String devicePath = getCacheKey(messageInfo);
        if (destinationCache.containsKey(devicePath)) {
            return destinationCache.get(devicePath);
        }

        String table = fetchMetadata(messageInfo, DESTINATION_TABLE_METADATA_PREFIX+messageInfo.getMessageType());
        if (table == null) {
            throw new RuntimeException(String.format("No destination table find for device: %s", devicePath));
        }

        String dataset = fetchMetadata(messageInfo, DESTINATION_DATASET_METADATA_PREFIX+messageInfo.getMessageType());
        if (dataset == null) {
            throw new RuntimeException(String.format("No destination dataset find for device: %s", devicePath));
        }

        TableDestination tableDestination = new TableDestination(
                new TableReference().setProjectId(messageInfo.getProjectId()).setDatasetId(dataset).setTableId(table),
                "Dynamically loaded destination");

        destinationCache.put(devicePath, tableDestination);

        return destinationCache.get(devicePath);
    }

    private static String fetchMetadata(IoTCoreMessageInfo messageInfo, String metadataKey) {
        try {
            return GCPIoTCoreUtil.getDeviceMetadata(messageInfo.getDeviceId(), messageInfo.getProjectId(),
                    messageInfo.getDeviceRegistryLocation(), messageInfo.getDeviceRegistryId()).get(metadataKey);
        } catch (Exception e) {
            return null;
        }
    }

    private static String getCacheKey(IoTCoreMessageInfo messageInfo) {
        return String.format("projects/%s/locations/%s/registries/%s/devices/%s/%s", messageInfo.getProjectId(),
                messageInfo.getDeviceRegistryLocation(), messageInfo.getDeviceRegistryId(), messageInfo.getDeviceId(), messageInfo.getMessageType());

    }
}