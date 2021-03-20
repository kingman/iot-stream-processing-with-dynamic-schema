package com.google.cloud.solutions.transformation;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.SchemaMapLoader;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;


import java.util.*;
import java.util.Map.Entry;

public class PubSubMessageToTableRowMapper extends DoFn<PubSubMessageWithMessageInfo, TableRowWithMessageInfo> {
    private static final String IOT_CORE_ATTRIBUTE_PREFIX = "cloudIoT.attr.";

    @ProcessElement
    public void processElement(@Element PubSubMessageWithMessageInfo message, OutputReceiver<TableRowWithMessageInfo> receiver) {
        Map<String, String> attributeMap = message.getMessage().getAttributeMap();
        JsonObject payloadJson = new JsonParser().parse(new String(message.getMessage().getPayload())).getAsJsonObject();
        JsonObject schemaMap = SchemaMapLoader.getSchemaMap(message.getMessageInfo());
        Map<String, TableFieldSchema> tableSchema = TableSchemaLoader.getFieldMap(message.getMessageInfo());
        List<Map<String, String>> rows = new ArrayList<>();
        Map<String, String> rowValues = new HashMap<>();
        rows.add(rowValues);
        parsePayload(payloadJson, schemaMap, attributeMap, rows);
        for(Map<String, String> row : rows) {
            receiver.output(new TableRowWithMessageInfo(message.getMessageInfo(), toTableRow(row, tableSchema)));
        }
    }

    private TableRow toTableRow(Map<String, String> row, Map<String, TableFieldSchema> tableSchema) {
        TableRow tableRow = new TableRow();
            for(String key: row.keySet()) {
                if(tableSchema.containsKey(key)) {
                    TableFieldSchema tableFieldSchema = tableSchema.get(key);
                    tableRow.put(key, createTableRowField(row.get(key), tableFieldSchema));
                }

        }
        return tableRow;
    }

    private Object createTableRowField(String valueStr, TableFieldSchema tableFieldSchema) {
        if("string".equalsIgnoreCase(tableFieldSchema.getType())) {
            return valueStr;
        }
        if("int64".equalsIgnoreCase(tableFieldSchema.getType())) {
            return Long.parseLong(valueStr);
        }
        if("timestamp".equalsIgnoreCase(tableFieldSchema.getType())) {
            try {
                long epochMilli = Long.parseLong(valueStr);
                if (valueStr.length() > 13) {
                    epochMilli = Long.parseLong(valueStr.substring(0,13));
                }
                return Instant.ofEpochMilli(epochMilli).toString();
            } catch (NumberFormatException nfe) {
                return valueStr;
            }
        }
        if("float".equalsIgnoreCase(tableFieldSchema.getType())) {
            return Double.parseDouble(valueStr);
        }
        return valueStr;
    }


    private void parsePayload(JsonObject payload, JsonObject mapper, Map<String, String> attributeMap, List<Map<String, String>> rows ) {
        for(Entry<String, JsonElement> entry : mapper.entrySet()) {
            if (entry.getKey().startsWith(IOT_CORE_ATTRIBUTE_PREFIX)) {
                String value = attributeMap.get(entry.getKey().substring(IOT_CORE_ATTRIBUTE_PREFIX.length()));
                if (value != null) {
                    for(Map<String, String> rowValues : rows) {
                        rowValues.put(entry.getValue().getAsString(), value);
                    }
                }
            }
            else {
                parsePayloadField(payload, entry.getKey(), entry.getValue(), attributeMap, rows);
            }
        }
    }

    private void parsePayloadField(JsonObject payloadJson, String payloadKey, JsonElement mapKey, Map<String, String> attributeMap, List<Map<String, String>> rows) {
        String[] keys = payloadKey.split("\\.");
        getFieldValue(payloadJson, keys, mapKey, attributeMap, rows);
    }

    private void getFieldValue(JsonObject payloadJson, String[] keys, JsonElement mapKey, Map<String, String> attributeMap, List<Map<String, String>> rows) {
        if(keys.length == 1) {
            if(keys[0].endsWith("[]")) {
                List<Map<String, String>> rowsHolder = new ArrayList<>();

                for (JsonElement jsonElement : payloadJson.get(keys[0].substring(0, keys[0].length() - 2)).getAsJsonArray()) {
                    JsonObject payloadArrayItem = jsonElement.getAsJsonObject();

                    for (Map<String, String> rowValues : rows) {
                        Map<String, String> rowCopy = new HashMap<>(rowValues);
                        List<Map<String, String>> rowsCopy = new ArrayList<>();
                        rowsCopy.add(rowCopy);
                        parsePayload(payloadArrayItem, mapKey.getAsJsonObject(), attributeMap, rowsCopy);
                        rowsHolder.addAll(rowsCopy);
                    }
                    rows.clear();
                    rows.addAll(rowsHolder);
                }
            } else {
                for(Map<String, String> rowValues : rows) {
                    rowValues.put(mapKey.getAsString(), payloadJson.get(keys[0]).getAsString());
                }
            }
        } else {
            getFieldValue(payloadJson.getAsJsonObject(keys[0]), subKeys(keys), mapKey, attributeMap, rows);
        }
    }

    private String[] subKeys(String[] keys) {
        String[] subKeys = new String[keys.length - 1];
        System.arraycopy(keys,1, subKeys, 0, subKeys.length);
        return subKeys;
    }
}
