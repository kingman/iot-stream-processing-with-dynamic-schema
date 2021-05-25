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
        final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
        if (destinationCache.containsKey(cacheKey)) {
            return destinationCache.get(cacheKey);
        }

        String table = GCPIoTCoreUtil.getMetaDataEntry(
                messageInfo, DESTINATION_TABLE_METADATA_PREFIX+messageInfo.getMessageType());
        if (table == null) {
            throw new RuntimeException(String.format("No destination table find for device: %s", cacheKey));
        }

        String dataset = GCPIoTCoreUtil.getMetaDataEntry(
                messageInfo, DESTINATION_DATASET_METADATA_PREFIX+messageInfo.getMessageType());
        if (dataset == null) {
            throw new RuntimeException(String.format("No destination dataset find for device: %s", cacheKey));
        }

        TableDestination tableDestination = new TableDestination(
                new TableReference().setProjectId(messageInfo.getProjectId()).setDatasetId(dataset).setTableId(table),
                "Dynamically loaded destination");

        destinationCache.put(cacheKey, tableDestination);

        return destinationCache.get(cacheKey);
    }

    public static void clearCache(IoTCoreMessageInfo messageInfo) {
        final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
        destinationCache.remove(cacheKey);
    }
}