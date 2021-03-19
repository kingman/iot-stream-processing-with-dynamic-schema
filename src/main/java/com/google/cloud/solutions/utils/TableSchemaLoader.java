package com.google.cloud.solutions.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.google.common.collect.ImmutableList;

public class TableSchemaLoader {
    private static final String TABLE_SCHEMA_METADATA_PREFIX = "table-schema-";
    private static final Map<String, TableSchema> schemaCache = new HashMap<>();
    private static final Map<String, Map<String, TableFieldSchema>> fieldMapCache = new HashMap<>();

    public static TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
        final String devicePath = getCacheKey(messageInfo);

        if (schemaCache.containsKey(devicePath)) {
            return schemaCache.get(devicePath);
        }

        String schemaStr = fetchMetadata(messageInfo, TABLE_SCHEMA_METADATA_PREFIX+messageInfo.getMessageType());
        if (schemaStr == null) {
            throw new RuntimeException(String.format("No table scheme find for device: %s", devicePath));
        }
        TableSchema schema = createScheme(schemaStr);
        schemaCache.put(devicePath, schema);
        return schemaCache.get(devicePath);

    }

    public static Map<String, TableFieldSchema> getFieldMap(IoTCoreMessageInfo messageInfo) {
        final String devicePath = getCacheKey(messageInfo);

        if (fieldMapCache.containsKey(devicePath)) {
            return fieldMapCache.get(devicePath);
        }
        String schemaStr = fetchMetadata(messageInfo, TABLE_SCHEMA_METADATA_PREFIX+messageInfo.getMessageType());
        if (schemaStr == null) {
            throw new RuntimeException(String.format("No table scheme find for device: %s", devicePath));
        }
        JsonArray fields = new JsonParser().parse(schemaStr).getAsJsonArray();
        List<TableFieldSchema> fieldSchemas = createFieldSchemaList(fields);
        Map<String, TableFieldSchema> fieldMap = new HashMap<>();
        for(TableFieldSchema tableFieldSchema : fieldSchemas) {
            fieldMap.put(tableFieldSchema.getName(), tableFieldSchema);
        }
        fieldMapCache.put(devicePath, fieldMap);
        return fieldMapCache.get(devicePath);

    }

    private static String getCacheKey(IoTCoreMessageInfo messageInfo) {
        return String.format("projects/%s/locations/%s/registries/%s/devices/%s/%s", messageInfo.getProjectId(),
                messageInfo.getDeviceRegistryLocation(), messageInfo.getDeviceRegistryId(), messageInfo.getDeviceId(), messageInfo.getMessageType());

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

    private static TableSchema createScheme(String schemaStr) {
        JsonArray fields = new JsonParser().parse(schemaStr).getAsJsonArray();
        List<TableFieldSchema> fieldSchemas = createFieldSchemaList(fields);
        return new TableSchema().setFields(ImmutableList.copyOf(fieldSchemas));
    }

    private static List<TableFieldSchema> createFieldSchemaList(JsonArray fields) {
        List<TableFieldSchema> fieldSchemas = new ArrayList<>();
        fields.forEach(field -> {
            JsonObject fieldObj = field.getAsJsonObject();
            TableFieldSchema tableFieldSchema = new TableFieldSchema()
            .setName(fieldObj.get("name").getAsString())
            .setType(fieldObj.get("type").getAsString())
            .setMode(fieldObj.get("mode").getAsString());
            if("RECORD".equalsIgnoreCase(fieldObj.get("type").getAsString())) {
                if(fieldObj.has("fields")) {
                    tableFieldSchema.setFields(createFieldSchemaList(fieldObj.get("fields").getAsJsonArray()));
                }
            }
            fieldSchemas.add(tableFieldSchema);
        });
        return fieldSchemas;
    }
}