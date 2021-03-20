package com.google.cloud.solutions.utils;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class InputDataSchemaValidator implements SerializableFunction<PubSubMessageWithMessageInfo, Boolean> {

    private static final long serialVersionUID = -2659650110307165972L;
    private static final String INPUT_DATA_SCHEMA_META_DATA_KEY = "input-data-schemas";
    private static final String SCHEMA_FIELD = "schema";
    private static final String DATA_TYPE_FIELD = "dataType";
    private final Map<String, Map<String, Schema>> schemaCache;

    public InputDataSchemaValidator() {
        this.schemaCache = new HashMap<>();
    }

    @Override
    public Boolean apply(PubSubMessageWithMessageInfo messageWithInfo) {
        Map<String, Schema> schemas = getSchemaList(messageWithInfo);
        if (schemas == null) { // not schema available to validate against
            return false;
        }
        JSONObject messageToValidate;

        try {
            messageToValidate = new JSONObject(new JSONTokener(new ByteArrayInputStream(messageWithInfo.getPayload())));
        } catch (Exception e) { //not valid json
            return false;
        }

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            try {
                entry.getValue().validate(messageToValidate);
                messageWithInfo.getMessageInfo().setMessageType(entry.getKey());
                return true;
            } catch (ValidationException ignored) {} //do not match schema requirement
        }
        return false;
    }

    private Map<String, Schema> getSchemaList(PubSubMessageWithMessageInfo message) {
        final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKey(message.getMessageInfo());

        if (schemaCache.containsKey(cacheKey)) {
            return schemaCache.get(cacheKey);
        }

        String schemaString = GCPIoTCoreUtil.getMetaDataEntry(message.getMessageInfo(), INPUT_DATA_SCHEMA_META_DATA_KEY);
        if (schemaString == null) { // not schema available to validate against
            schemaCache.put(cacheKey, null);
            return null;
        }

        Map<String, Schema> schemaMap = new HashMap<>();

        JsonArray jsonSchemas = new JsonParser().parse(schemaString).getAsJsonArray();
        jsonSchemas.forEach(jsonSchema -> {
            JsonObject schemaObject = jsonSchema.getAsJsonObject();
            JSONObject schemaJsonObject = new JSONObject(new JSONTokener(new ByteArrayInputStream(schemaObject.get(SCHEMA_FIELD).toString().getBytes())));
            Schema schema = SchemaLoader.load(schemaJsonObject);
            String dataType = schemaObject.get(DATA_TYPE_FIELD).getAsString();
            schemaMap.put(dataType, schema);
        });

        schemaCache.put(cacheKey, schemaMap);
        return schemaCache.get(cacheKey);
    }
}
