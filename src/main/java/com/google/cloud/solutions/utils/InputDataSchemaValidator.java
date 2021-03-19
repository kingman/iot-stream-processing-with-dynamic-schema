package com.google.cloud.solutions.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class InputDataSchemaValidator implements SerializableFunction<PubsubMessage, Boolean> {

    private static final long serialVersionUID = -2659650110307165972L;
    private static final String INPUT_DATA_SCHEMA_META_DATA_KEY = "input-data-schemas";
    private static final String SCHEMA_FIELD = "schema";
    private static final String DATA_TYPE_FIELD = "dataType";
    private static final String IOT_MESSAGE_TYPE = "iotMessageType";
    private static final String UNKNOWN_MESSAGE_TYPE = "unknown";
    private final Map<String, Map<String, Schema>> schemaCache;

    public InputDataSchemaValidator() {
        this.schemaCache = new HashMap<>();
    }

    @Override
    public Boolean apply(PubsubMessage message) {
        Map<String, Schema> schemas = getSchemaList(message);
        if (schemas == null) { // not schema available to validate against
            message.getAttributeMap().put(IOT_MESSAGE_TYPE, UNKNOWN_MESSAGE_TYPE);
            return false;
        }
        JSONObject messageToValidate;

        try {
            messageToValidate = new JSONObject(new JSONTokener(new ByteArrayInputStream(message.getPayload())));
        } catch (Exception e) { //not valid json
            message.getAttributeMap().put(IOT_MESSAGE_TYPE, UNKNOWN_MESSAGE_TYPE);
            return false;
        }

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
            try {
                entry.getValue().validate(messageToValidate);
                message.getAttributeMap().put(IOT_MESSAGE_TYPE, entry.getKey());
                return true;
            } catch (ValidationException ignored) {}
        }
        message.getAttributeMap().put(IOT_MESSAGE_TYPE, UNKNOWN_MESSAGE_TYPE);
        return false;
    }

    private Map<String, Schema> getSchemaList(PubsubMessage message) {
        final String devicePath = getCacheKey(message);

        if (schemaCache.containsKey(devicePath)) {
            return schemaCache.get(devicePath);
        }

        String schemaString = getSchemaString(message);
        if (schemaString == null) { // not schema available to validate against
            schemaCache.put(devicePath, null);
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

        schemaCache.put(devicePath, schemaMap);
        return schemaCache.get(devicePath);
    }

    private String getCacheKey(PubsubMessage message) {
        return String.format("projects/%s/locations/%s/registries/%s/devices/%s", message.getAttribute("projectId"),
                message.getAttribute("deviceRegistryLocation"), message.getAttribute("deviceRegistryId"),
                message.getAttribute("deviceId"));

    }

    private String getSchemaString(PubsubMessage message) {
        try {
            return GCPIoTCoreUtil
                    .getDeviceMetadata(message.getAttribute("deviceId"), message.getAttribute("projectId"),
                            message.getAttribute("deviceRegistryLocation"), message.getAttribute("deviceRegistryId"))
                    .get(INPUT_DATA_SCHEMA_META_DATA_KEY);
        } catch (Exception e) {
            return null;
        }

    }

}
