package io.etcdhbase.server.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationSchemaTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void schemaPropertiesAndRequiredFieldsMatchRuntimeRecords() throws IOException {
    try (InputStream input = getClass().getResourceAsStream("/m0-schema/server.schema.json")) {
      assertTrue(input != null, "server.schema.json must be available to tests");
      JsonNode schema = JSON.readTree(input);
      assertRecordSchema(schema, schema, RuntimeConfiguration.class);
    }
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
    if (recordType == RuntimeConfiguration.HBaseProfile.class) {
      expectedRequiredNames.remove("credential_reference");
    }
    Set<String> requiredNames = new HashSet<>();
    resolved.path("required").forEach(value -> requiredNames.add(value.asText()));
    assertEquals(expectedRequiredNames, requiredNames, recordType.getSimpleName() + " required fields");

    for (RecordComponent component : recordType.getRecordComponents()) {
      assertTypeSchema(
          root,
          properties.path(toSnakeCase(component.getName())),
          component.getGenericType(),
          recordType.getSimpleName() + "." + component.getName());
    }
  }

  private static void assertTypeSchema(JsonNode root, JsonNode fieldSchema, Type fieldType, String path) {
    if (fieldType instanceof ParameterizedType parameterized
        && parameterized.getRawType() == List.class) {
      JsonNode resolved = resolve(root, fieldSchema);
      assertEquals("array", resolved.path("type").asText(), path);
      assertTypeSchema(root, resolved.path("items"), parameterized.getActualTypeArguments()[0], path + "[]");
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
