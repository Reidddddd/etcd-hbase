package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RuntimeConfigurationSchemaTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void schemaPropertiesAndRequiredFieldsMatchRuntimeRecords() throws IOException {
    try (InputStream input =
        java.nio.file.Files.newInputStream(
            java.nio.file.Path.of("../config/schema/server.schema.json"))) {
      assertTrue(input != null, "server.schema.json must be available to tests");
      JsonNode schema = JSON.readTree(input);
      assertRecordSchema(schema, schema, RuntimeConfiguration.class);
    }
  }

  @Test
  void schemaDescribesTheSharedColumnFamilyAndCollectionBounds() throws IOException {
    JsonNode schema =
        JSON.readTree(
            java.nio.file.Files.readAllBytes(
                java.nio.file.Path.of("../config/schema/server.schema.json")));
    JsonNode family = schema.path("$defs").path("ColumnFamilySchema").path("properties");
    assertEquals(32, family.path("name").path("maxLength").intValue());
    String pattern = family.path("name").path("pattern").textValue();
    for (String invalid : List.of("2data", "data.column", "a".repeat(33))) {
      assertTrue(!invalid.matches(pattern));
    }
    assertTrue("a".repeat(32).matches(pattern));
    assertEquals(1024, family.path("max_versions").path("maximum").intValue());
    assertEquals(1024, family.path("min_versions").path("maximum").intValue());
    assertEquals(
        Integer.MAX_VALUE - 1,
        family.path("ttl_seconds").path("oneOf").path(1).path("maximum").intValue());
    assertEquals(32, schema.path("properties").path("table_groups").path("maxItems").intValue());
    assertEquals(
        1024,
        schema
            .path("$defs")
            .path("SaltProfile")
            .path("properties")
            .path("bucket_count")
            .path("maximum")
            .intValue());
  }

  @ParameterizedTest(name = "raw listener host length case {index}")
  @MethodSource("listenerHostLengthCases")
  void schemaAndLoaderAgreeOnRawListenerHostLength(String host) throws Exception {
    JsonNode schema =
        JSON.readTree(
            java.nio.file.Files.readAllBytes(
                java.nio.file.Path.of("../config/schema/server.schema.json")));
    JsonNode hostSchema = schema.path("$defs").path("Listener").path("properties").path("bind_host");
    assertEquals(253, hostSchema.path("maxLength").intValue());
    var configuration = JSON.readTree(RuntimeConfigurationLoaderTest.example());
    ((ObjectNode) configuration.path("hbase_profiles").path(0))
        .put("authentication_mode", "CREDENTIAL_REFERENCE")
        .put("credential_reference", "env://TEST_REFERENCE");
    ((ObjectNode) configuration.path("table_groups").path(0))
        .put("identity_provider_id", "custom-provider");
    ((ObjectNode) configuration.path("listeners").path(0))
        .put("bind_host", host)
        .put("port", 12345);
    byte[] input = JSON.writeValueAsBytes(configuration);
    var loader = new RuntimeConfigurationLoader();
    int rawLength = host.codePointCount(0, host.length());
    boolean schemaAccepts =
        rawLength >= hostSchema.path("minLength").intValue()
            && rawLength <= hostSchema.path("maxLength").intValue();
    if (schemaAccepts) {
      var loaded = loader.load(new ByteArrayInputStream(input), Map.of());
      assertEquals(host, loaded.listeners().getFirst().bindHost());
      return;
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> loader.load(new ByteArrayInputStream(input), Map.of()));
  }

  private static Stream<String> listenerHostLengthCases() {
    return Stream.of(60, 61, 62)
        .flatMap(
            lastLabelLength -> {
              String host = ("a".repeat(63) + ".").repeat(3) + "a".repeat(lastLabelLength);
              return Stream.of(host, host + ".");
            });
  }

  private static void assertRecordSchema(JsonNode root, JsonNode schemaNode, Class<?> recordType) {
    JsonNode resolved = resolve(root, schemaNode);
    assertEquals("object", resolved.path("type").asText(), recordType.getSimpleName());
    assertTrue(
        resolved.path("additionalProperties").isBoolean()
            && !resolved.path("additionalProperties").asBoolean(),
        recordType.getSimpleName() + " must reject unknown fields");

    JsonNode properties = resolved.path("properties");
    Set<String> expectedNames = new HashSet<>();
    for (RecordComponent component : recordType.getRecordComponents()) {
      expectedNames.add(toSnakeCase(component.getName()));
    }
    Set<String> schemaNames = new HashSet<>();
    Iterator<String> names = properties.fieldNames();
    names.forEachRemaining(schemaNames::add);
    assertEquals(expectedNames, schemaNames, recordType.getSimpleName() + " schema properties");

    Set<String> expectedRequiredNames = new HashSet<>(expectedNames);
    if (recordType == io.etcdhbase.admin.config.InitializationConfiguration.HBaseProfile.class) {
      expectedRequiredNames.remove("credential_reference");
    }
    for (RecordComponent component : recordType.getRecordComponents()) {
      var property = component.getAccessor().getAnnotation(
          com.fasterxml.jackson.annotation.JsonProperty.class);
      if (property != null && !property.required()) {
        expectedRequiredNames.remove(toSnakeCase(component.getName()));
      }
    }
    Set<String> requiredNames = new HashSet<>();
    resolved.path("required").forEach(value -> requiredNames.add(value.asText()));
    assertEquals(
        expectedRequiredNames, requiredNames, recordType.getSimpleName() + " required fields");

    for (RecordComponent component : recordType.getRecordComponents()) {
      assertTypeSchema(
          root,
          properties.path(toSnakeCase(component.getName())),
          component.getGenericType(),
          recordType.getSimpleName() + "." + component.getName());
    }
  }

  private static void assertTypeSchema(
      JsonNode root, JsonNode fieldSchema, Type fieldType, String path) {
    if (fieldType instanceof ParameterizedType parameterized
        && parameterized.getRawType() == List.class) {
      JsonNode resolved = resolve(root, fieldSchema);
      assertEquals("array", resolved.path("type").asText(), path);
      assertTypeSchema(
          root, resolved.path("items"), parameterized.getActualTypeArguments()[0], path + "[]");
      return;
    }
    if (!(fieldType instanceof Class<?> javaType)) {
      throw new AssertionError("Unsupported runtime type at " + path + ": " + fieldType);
    }
    if (javaType.isRecord()) {
      assertRecordSchema(root, fieldSchema, javaType);
    } else if (javaType.isEnum()) {
      JsonNode resolved = resolve(root, fieldSchema);
      assertEquals("string", resolved.path("type").asText(), path);
      Set<String> expectedValues = new HashSet<>();
      for (Object constant : javaType.getEnumConstants()) {
        expectedValues.add(((Enum<?>) constant).name());
      }
      Set<String> schemaValues = new HashSet<>();
      resolved.path("enum").forEach(value -> schemaValues.add(value.asText()));
      assertEquals(expectedValues, schemaValues, path + " enum values");
    } else if (javaType == String.class) {
      assertEquals("string", resolve(root, fieldSchema).path("type").asText(), path);
    } else if (javaType == int.class || javaType == long.class) {
      assertEquals("integer", resolve(root, fieldSchema).path("type").asText(), path);
    } else {
      throw new AssertionError("Unsupported runtime type at " + path + ": " + fieldType);
    }
  }

  private static JsonNode resolve(JsonNode root, JsonNode schemaNode) {
    JsonNode reference = schemaNode.get("$ref");
    if (reference == null) {
      return schemaNode;
    }
    String pointer = reference.asText();
    if (!pointer.startsWith("#/$defs/")) {
      throw new AssertionError("Unsupported schema reference: " + pointer);
    }
    return root.path("$defs").path(pointer.substring("#/$defs/".length()));
  }

  private static String toSnakeCase(String value) {
    StringBuilder result = new StringBuilder();
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (Character.isUpperCase(character)) {
        result.append('_').append(Character.toLowerCase(character));
      } else {
        result.append(character);
      }
    }
    return result.toString();
  }
}
