package com.google.cloud.solutions.transformation;

import static org.junit.Assert.assertEquals;

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.utils.GCPIoTCoreUtil;
import com.google.cloud.solutions.utils.InputDataSchemaValidator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class IoTMessageSchemaValidationTest {
  @Test
  public void invalidInputTest() throws IOException {
    InputDataSchemaValidator cut = new InputDataSchemaValidator();
    PubSubMessageWithMessageInfo input =
        new PubSubMessageWithMessageInfo(
            new PubsubMessage("test invalid message".getBytes(), generateAttributeMap()));
    try (MockedStatic<GCPIoTCoreUtil> schemaLoaderMock = Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "input-data-schemas"))
          .thenReturn(loadInputSchema());
      boolean validationResult = cut.apply(input);
      assertEquals(false, validationResult);
    }
  }

  @Test
  public void validInput() throws IOException {
    InputDataSchemaValidator cut = new InputDataSchemaValidator();
    PubSubMessageWithMessageInfo input =
        new PubSubMessageWithMessageInfo(
            new PubsubMessage(generateValidMessage(), generateAttributeMap()));

    try (MockedStatic<GCPIoTCoreUtil> schemaLoaderMock = Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "input-data-schemas"))
          .thenReturn(loadInputSchema());

      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "schema-map-edgex"))
          .thenReturn(loadSchemaMap());

      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "table-schema-edgex"))
          .thenReturn(loadTableSchema());

      schemaLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      input.getMessageInfo(), "destination-table-edgex"))
          .thenReturn("test-table");

      schemaLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      input.getMessageInfo(), "destination-dataset-edgex"))
          .thenReturn("test-dataset");

      boolean validationResult = cut.apply(input);
      assertEquals(true, validationResult);
    }
  }

  @Test
  public void validInputWithIncompleteConfiguration() throws IOException {
    InputDataSchemaValidator cut = new InputDataSchemaValidator();
    PubSubMessageWithMessageInfo input =
        new PubSubMessageWithMessageInfo(
            new PubsubMessage(generateValidMessage(), generateAttributeMap()));

    try (MockedStatic<GCPIoTCoreUtil> schemaLoaderMock = Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "input-data-schemas"))
          .thenReturn(loadInputSchema());

      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "schema-map-edgex"))
          .thenReturn(loadSchemaMap());

      schemaLoaderMock
          .when(() -> GCPIoTCoreUtil.getMetaDataEntry(input.getMessageInfo(), "table-schema-edgex"))
          .thenReturn(loadTableSchema());

      schemaLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      input.getMessageInfo(), "destination-table-edgex"))
          .thenReturn("test-table");

      boolean validationResult = cut.apply(input);
      assertEquals(false, validationResult);
    }
  }

  private static Map<String, String> generateAttributeMap() {
    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("deviceRegistryLocation", "europe-west1");
    attributeMap.put("deviceRegistryId", "test-reg");
    attributeMap.put("deviceId", "test-device");
    attributeMap.put("projectId", "test-project");
    return attributeMap;
  }

  private static byte[] generateValidMessage() {
    long now = System.currentTimeMillis();
    String deviceName = "Test Device";
    String id = UUID.randomUUID().toString();
    JSONArray measurements =
        new JSONArray()
            .put(
                generateSingleMeasurement(id, now, deviceName, "TestMeasureType", "42.5", "FLOAT"));

    return new JSONObject()
        .put("id", id)
        .put("device", deviceName)
        .put("created", now)
        .put("origin", now)
        .put("readings", measurements)
        .toString()
        .getBytes();
  }

  private static JSONObject generateSingleMeasurement(
      String id,
      Long timestamp,
      String deviceName,
      String measurementType,
      String measurementVal,
      String valueTypeStr) {
    return new JSONObject()
        .put("id", id)
        .put("origin", timestamp)
        .put("device", deviceName)
        .put("name", measurementType)
        .put("value", measurementVal)
        .put("valueType", valueTypeStr);
  }

  private static String loadInputSchema() throws IOException {
    String path = "data-configs/input-data-schema.json";
    return readFile(path);
  }

  private static String loadSchemaMap() throws IOException {
    String path = "data-configs/edgex-schema-mapping.json";
    return readFile(path);
  }

  private static String loadTableSchema() throws IOException {
    String path = "data-configs/edgex-table-schema.json";
    return readFile(path);
  }

  private static String readFile(String path) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, StandardCharsets.UTF_8);
  }
}
