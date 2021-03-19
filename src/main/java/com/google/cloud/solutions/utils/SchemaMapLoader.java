package com.google.cloud.solutions.utils;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


import java.util.HashMap;
import java.util.Map;

public class SchemaMapLoader {
    private static final Map<String, JsonObject> mapCache = new HashMap<>();
    private static final String SCHEMA_MAP_METADATA_KEY = "schema-map";

    public static JsonObject getSchemaMap(IoTCoreMessageInfo messageInfo) {
        final String devicePath = getCacheKey(messageInfo);

        if (mapCache.containsKey(devicePath)) {
            return mapCache.get(devicePath);
        }

        String mapStr = fetchMetadata(messageInfo, SCHEMA_MAP_METADATA_KEY);
        if (mapStr == null) {
            throw new RuntimeException(String.format("No table scheme find for device: %s", devicePath));
        }
        JsonObject map = new JsonParser().parse(mapStr).getAsJsonObject();
        mapCache.put(devicePath, map);
        return mapCache.get(devicePath);
    }

    private static String getCacheKey(IoTCoreMessageInfo messageInfo) {
        return String.format("projects/%s/locations/%s/registries/%s/devices/%s", messageInfo.getProjectId(),
                messageInfo.getDeviceRegistryLocation(), messageInfo.getDeviceRegistryId(), messageInfo.getDeviceId());

    }

    private static String fetchMetadata(IoTCoreMessageInfo messageInfo, String metadataKey) {
        try {
            return GCPIoTCoreUtil
                    .getDeviceMetadata(messageInfo.getDeviceId(), messageInfo.getProjectId(),
                            messageInfo.getDeviceRegistryLocation(), messageInfo.getDeviceRegistryId())
                    .get(metadataKey);
        } catch (Exception e) {
            return null;
        }
    }
}
