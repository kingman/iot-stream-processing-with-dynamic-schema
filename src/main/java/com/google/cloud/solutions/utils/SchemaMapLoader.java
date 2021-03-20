package com.google.cloud.solutions.utils;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


import java.util.HashMap;
import java.util.Map;

public class SchemaMapLoader {
    private static final Map<String, JsonObject> mapCache = new HashMap<>();
    private static final String SCHEMA_MAP_METADATA_PREFIX = "schema-map-";

    public static JsonObject getSchemaMap(IoTCoreMessageInfo messageInfo) {
        final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);

        if (mapCache.containsKey(cacheKey)) {
            return mapCache.get(cacheKey);
        }

        String mapStr = GCPIoTCoreUtil.getMetaDataEntry(
                messageInfo, SCHEMA_MAP_METADATA_PREFIX+messageInfo.getMessageType());
        if (mapStr == null) {
            throw new RuntimeException(String.format("No table scheme find for device: %s", cacheKey));
        }
        JsonObject map = new JsonParser().parse(mapStr).getAsJsonObject();
        mapCache.put(cacheKey, map);
        return mapCache.get(cacheKey);
    }
}
