package io.etcdhbase.server.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

/** Reads and validates a bounded YAML runtime configuration without exposing input values on error. */
public final class RuntimeConfigurationLoader {
  public static final int ABSOLUTE_MAX_CONFIG_BYTES = 64 * 1024;
  private static final int MAX_YAML_NESTING = 32;
  private static final String ENVIRONMENT_PREFIX = "ETCD_HBASE_";
  private static final Set<String> BUILD_ENVIRONMENT_VARIABLES =
      Set.of("ETCD_HBASE_JDK21_HOME", "ETCD_HBASE_MAVEN_REPOSITORY");
  private static final Map<String, EnvironmentOverride> ENVIRONMENT_OVERRIDES = environmentOverrides();
  private final RuntimeConfigurationValidator validator;
  private final ObjectMapper objectMapper;
  private final Supplier<Map<String, String>> environmentSupplier;

  public RuntimeConfigurationLoader(
      Set<String> supportedProviderIds, Set<String> supportedSaltStrategyIds) {
    this(supportedProviderIds, supportedSaltStrategyIds, System::getenv);
  }

  RuntimeConfigurationLoader(
      Set<String> supportedProviderIds,
      Set<String> supportedSaltStrategyIds,
      Supplier<Map<String, String>> environmentSupplier) {
    this.validator = new RuntimeConfigurationValidator(supportedProviderIds, supportedSaltStrategyIds);
    if (environmentSupplier == null) {
      throw new IllegalArgumentException("environment supplier is required");
    }
    this.environmentSupplier = environmentSupplier;
    this.objectMapper =
        JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .build();
  }

  /** Reads up to the hard limit plus one byte. The caller retains ownership of the stream. */
  public RuntimeConfiguration load(InputStream input) {
    return load(input, environmentSupplier.get());
  }

  /** Reads one configuration and applies only the documented environment overrides. */
  public RuntimeConfiguration load(InputStream input, Map<String, String> environment) {
    if (input == null) {
      throw invalid("configuration", "input is required");
    }
    try {
      byte[] bytes = input.readNBytes(ABSOLUTE_MAX_CONFIG_BYTES + 1);
      if (bytes.length > ABSOLUTE_MAX_CONFIG_BYTES) {
        throw invalid("configuration", "exceeds the 65536-byte limit");
      }
      return load(bytes, environment);
    } catch (IOException exception) {
      throw invalid("configuration", "could not be read");
    }
  }

  /** Parses and validates one UTF-8 YAML document. */
  public RuntimeConfiguration load(byte[] input) {
    return load(input, environmentSupplier.get());
  }

  /** Parses YAML, overlays documented environment values, then applies the shared validator. */
  public RuntimeConfiguration load(byte[] input, Map<String, String> environment) {
    if (input == null) {
      throw invalid("configuration", "input is required");
    }
    if (input.length == 0) {
      throw invalid("configuration", "must not be empty");
    }
    if (input.length > ABSOLUTE_MAX_CONFIG_BYTES) {
      throw invalid("configuration", "exceeds the 65536-byte limit");
    }

    String yamlText = decodeUtf8(input);
    Object parsed = parseSingleDocument(yamlText);
    if (!(parsed instanceof Map<?, ?> rawRoot)) {
      throw invalid("configuration", "root must be a mapping");
    }

    Object safeTree = validateYamlTree(rawRoot, "configuration", 0);
    EnvironmentResult effective = applyEnvironmentOverrides(safeTree, input.length, environment);
    validateRequiredProperties(effective.tree(), RuntimeConfiguration.class, "");
    RuntimeConfiguration configuration = bind(effective.tree());
    validator.validate(configuration, effective.byteCount());
    return configuration;
  }

  private static String decodeUtf8(byte[] input) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(input))
          .toString();
    } catch (CharacterCodingException exception) {
      throw invalid("configuration", "must be valid UTF-8");
    }
  }

  /**
   * Checks required record properties before Jackson binding can supply primitive defaults.
   * RuntimeConfigurationSchemaTest keeps the record fields aligned with the schema; the sole
   * omitted-field exception is the conditional credential reference.
   */
  private static void validateRequiredProperties(Object value, Class<?> recordType, String path) {
    if (!(value instanceof Map<?, ?> properties)) {
      return;
    }
    for (RecordComponent component : recordType.getRecordComponents()) {
      String propertyName = toSnakeCase(component.getName());
      String propertyPath = appendPropertyPath(path, propertyName);
      boolean optional =
          recordType == RuntimeConfiguration.HBaseProfile.class
              && component.getName().equals("credentialReference");
      if (!properties.containsKey(propertyName)) {
        if (!optional) {
          throw invalid(propertyPath, "is required");
        }
        continue;
      }

      Object child = properties.get(propertyName);
      if (child == null) {
        throw invalid(propertyPath, "must not be null");
      }
      if (component.getType().isRecord()) {
        validateRequiredProperties(child, component.getType(), propertyPath);
        continue;
      }
      Class<?> elementType = recordListElementType(component);
      if (elementType != null && child instanceof List<?> values) {
        for (int index = 0; index < values.size(); index++) {
          Object item = values.get(index);
          String itemPath = propertyPath + "[" + index + "]";
          if (item == null) {
            throw invalid(itemPath, "is required");
          }
          validateRequiredProperties(item, elementType, itemPath);
        }
      }
    }
  }

  private static Class<?> recordListElementType(RecordComponent component) {
    if (!List.class.isAssignableFrom(component.getType())
        || !(component.getGenericType() instanceof ParameterizedType listType)) {
      return null;
    }
    Type elementType = listType.getActualTypeArguments()[0];
    if (elementType instanceof Class<?> elementClass && elementClass.isRecord()) {
      return elementClass;
    }
    return null;
  }

  private static String toSnakeCase(String name) {
    StringBuilder result = new StringBuilder();
    for (int index = 0; index < name.length(); index++) {
      char character = name.charAt(index);
      if (Character.isUpperCase(character)) {
        result.append('_').append(Character.toLowerCase(character));
      } else {
        result.append(character);
      }
    }
    return result.toString();
  }

  private static String appendPropertyPath(String parent, String property) {
    return parent.isEmpty() ? property : parent + "." + property;
  }

  private static Object parseSingleDocument(String yamlText) {
    LoaderOptions options = new LoaderOptions();
    options.setAllowDuplicateKeys(false);
    options.setAllowRecursiveKeys(false);
    options.setMaxAliasesForCollections(0);
    options.setNestingDepthLimit(MAX_YAML_NESTING);
    options.setCodePointLimit(ABSOLUTE_MAX_CONFIG_BYTES);
    options.setMergeOnCompose(false);

    try {
      Iterator<Object> documents = new Yaml(new SafeConstructor(options)).loadAll(yamlText).iterator();
      if (!documents.hasNext()) {
        throw invalid("configuration", "must contain one YAML document");
      }
      Object document = documents.next();
      if (documents.hasNext()) {
        throw invalid("configuration", "must contain exactly one YAML document");
      }
      return document;
    } catch (YAMLException exception) {
      String location = "configuration";
      if (exception instanceof MarkedYAMLException marked && marked.getProblemMark() != null) {
        location +=
            " at line "
                + (marked.getProblemMark().getLine() + 1)
                + ", column "
                + (marked.getProblemMark().getColumn() + 1);
      }
      throw invalid(location, "contains invalid or unsupported YAML");
    }
  }

  private static Object validateYamlTree(Object value, String path, int depth) {
    if (depth > MAX_YAML_NESTING) {
      throw invalid(path, "exceeds the nesting limit");
    }
    if (value == null) {
      throw invalid(path, "must not be null");
    }
    if (value instanceof String || value instanceof Boolean || value instanceof Number) {
      return value;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (int index = 0; index < list.size(); index++) {
        if (list.get(index) == null) {
          throw invalid(path + "[" + index + "]", "must not be null");
        }
        copy.add(validateYamlTree(list.get(index), path + "[" + index + "]", depth + 1));
      }
      return List.copyOf(copy);
    }
    if (value instanceof Map<?, ?> mapping) {
      Map<String, Object> copy = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : mapping.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw invalid(path, "mapping keys must be strings");
        }
        copy.put(key, validateYamlTree(entry.getValue(), appendPath(path, key), depth + 1));
      }
      return Collections.unmodifiableMap(copy);
    }
    throw invalid(path, "contains an unsupported YAML value");
  }

  private static EnvironmentResult applyEnvironmentOverrides(
      Object safeTree, int fileByteCount, Map<String, String> environment) {
    if (environment == null) {
      throw invalid("environment", "map is required");
    }
    Map<String, String> selected = new TreeMap<>();
    for (Map.Entry<String, String> entry : environment.entrySet()) {
      String name = entry.getKey();
      if (name == null) {
        throw invalid("environment", "contains an invalid variable name");
      }
      if (!name.startsWith(ENVIRONMENT_PREFIX)) {
        continue;
      }
      if (BUILD_ENVIRONMENT_VARIABLES.contains(name)) {
        continue;
      }
      if (!ENVIRONMENT_OVERRIDES.containsKey(name)) {
        throw invalid("environment", "contains an unsupported ETCD_HBASE_ override");
      }
      selected.put(name, entry.getValue());
    }

    Map<String, Object> effectiveTree = mutableMap(mutableCopy(safeTree));
    long effectiveBytes = fileByteCount;
    for (Map.Entry<String, String> entry : selected.entrySet()) {
      EnvironmentOverride override = ENVIRONMENT_OVERRIDES.get(entry.getKey());
      String value = entry.getValue();
      if (value == null) {
        throw invalid(override.path(), "environment override must not be null");
      }
      if (value.length() > ABSOLUTE_MAX_CONFIG_BYTES) {
        throw invalid("configuration", "file and environment overrides exceed the 65536-byte limit");
      }
      effectiveBytes +=
          entry.getKey().getBytes(StandardCharsets.UTF_8).length
              + (long) value.getBytes(StandardCharsets.UTF_8).length
              + 2;
      if (effectiveBytes > ABSOLUTE_MAX_CONFIG_BYTES) {
        throw invalid("configuration", "file and environment overrides exceed the 65536-byte limit");
      }
      Object parsedValue = parseEnvironmentValue(value, override);
      setOverride(effectiveTree, override, parsedValue);
    }
    return new EnvironmentResult(effectiveTree, (int) effectiveBytes);
  }

  private static Object parseEnvironmentValue(String value, EnvironmentOverride override) {
    if (override.valueType() == EnvironmentValueType.STRING) {
      return value;
    }
    if (!value.matches("-?[0-9]+")) {
      throw invalid(override.path(), "environment override must be a decimal integer");
    }
    try {
      if (override.valueType() == EnvironmentValueType.INTEGER) {
        return Integer.valueOf(value);
      }
      return Long.valueOf(value);
    } catch (NumberFormatException exception) {
      throw invalid(override.path(), "environment override is outside the supported range");
    }
  }

  private static void setOverride(
      Map<String, Object> root, EnvironmentOverride override, Object value) {
    Object parent = root;
    List<String> segments = override.segments();
    for (int index = 0; index < segments.size() - 1; index++) {
      parent = child(parent, segments.get(index), override.path());
      if (parent == null) {
        throw invalid(override.path(), "cannot be overridden because its configuration section is missing");
      }
    }
    if (!(parent instanceof Map<?, ?>)) {
      throw invalid(override.path(), "cannot be overridden because its configuration section is invalid");
    }
    Map<String, Object> parentMap = mutableMap(parent);
    String field = segments.getLast();
    if (!parentMap.containsKey(field)) {
      throw invalid(override.path(), "cannot be overridden because the field is missing from the file");
    }
    parentMap.put(field, value);
  }

  private static Object child(Object parent, String segment, String path) {
    if (parent instanceof Map<?, ?> map) {
      return map.get(segment);
    }
    if (parent instanceof List<?> list) {
      try {
        int index = Integer.parseInt(segment);
        return index >= 0 && index < list.size() ? list.get(index) : null;
      } catch (NumberFormatException exception) {
        throw invalid(path, "contains an invalid collection path");
      }
    }
    throw invalid(path, "cannot be overridden because its configuration section is invalid");
  }

  private static Object mutableCopy(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> copy = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        copy.put((String) entry.getKey(), mutableCopy(entry.getValue()));
      }
      return copy;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (Object item : list) {
        copy.add(mutableCopy(item));
      }
      return copy;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> mutableMap(Object value) {
    return (Map<String, Object>) value;
  }

  private static Map<String, EnvironmentOverride> environmentOverrides() {
    return Map.ofEntries(
        Map.entry(
            "ETCD_HBASE_LISTENER_BIND_HOST",
            new EnvironmentOverride(
                "listeners[0].bind_host",
                List.of("listeners", "0", "bind_host"),
                EnvironmentValueType.STRING)),
        Map.entry(
            "ETCD_HBASE_LISTENER_PORT",
            new EnvironmentOverride(
                "listeners[0].port", List.of("listeners", "0", "port"), EnvironmentValueType.INTEGER)),
        Map.entry(
            "ETCD_HBASE_LISTENER_TABLE_GROUP_ID",
            new EnvironmentOverride(
                "listeners[0].table_group_id",
                List.of("listeners", "0", "table_group_id"),
                EnvironmentValueType.STRING)),
        Map.entry(
            "ETCD_HBASE_MAX_CONFIG_BYTES",
            new EnvironmentOverride(
                "resources.max_config_bytes",
                List.of("resources", "max_config_bytes"),
                EnvironmentValueType.INTEGER)),
        Map.entry(
            "ETCD_HBASE_MAX_CONCURRENT_REQUESTS",
            new EnvironmentOverride(
                "resources.max_concurrent_requests",
                List.of("resources", "max_concurrent_requests"),
                EnvironmentValueType.INTEGER)),
        Map.entry(
            "ETCD_HBASE_MAX_SCANNERS_PER_TABLE",
            new EnvironmentOverride(
                "resources.max_scanners_per_table",
                List.of("resources", "max_scanners_per_table"),
                EnvironmentValueType.INTEGER)),
        Map.entry(
            "ETCD_HBASE_REQUEST_DEADLINE_MS",
            new EnvironmentOverride(
                "resources.request_deadline_ms",
                List.of("resources", "request_deadline_ms"),
                EnvironmentValueType.INTEGER)),
        Map.entry(
            "ETCD_HBASE_MAX_RPC_BYTES",
            new EnvironmentOverride(
                "resources.max_rpc_bytes",
                List.of("resources", "max_rpc_bytes"),
                EnvironmentValueType.LONG)),
        Map.entry(
            "ETCD_HBASE_MAX_REQUEST_BYTES",
            new EnvironmentOverride(
                "resources.max_request_bytes",
                List.of("resources", "max_request_bytes"),
                EnvironmentValueType.LONG)),
        Map.entry(
            "ETCD_HBASE_MAX_QUEUED_REQUESTS",
            new EnvironmentOverride(
                "resources.max_queued_requests",
                List.of("resources", "max_queued_requests"),
                EnvironmentValueType.INTEGER)));
  }

  private RuntimeConfiguration bind(Object safeTree) {
    try {
      return objectMapper.convertValue(safeTree, RuntimeConfiguration.class);
    } catch (RuntimeException exception) {
      JsonMappingException mappingException = findMappingException(exception);
      String path = mappingException == null ? "configuration" : sanitizedPath(mappingException);
      throw invalid(path, "has an invalid or unsupported value");
    }
  }

  private static JsonMappingException findMappingException(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof JsonMappingException mappingException) {
        return mappingException;
      }
      current = current.getCause();
    }
    return null;
  }

  private static String sanitizedPath(JsonMappingException exception) {
    StringBuilder path = new StringBuilder("configuration");
    for (JsonMappingException.Reference reference : exception.getPath()) {
      if (reference.getFieldName() != null) {
        path.append('.').append(sanitizePathToken(reference.getFieldName()));
      } else if (reference.getIndex() >= 0) {
        path.append('[').append(reference.getIndex()).append(']');
      }
    }
    return path.toString();
  }

  private static String sanitizePathToken(String token) {
    StringBuilder sanitized = new StringBuilder();
    for (int index = 0; index < token.length() && sanitized.length() < 64; index++) {
      char character = token.charAt(index);
      if (Character.isLetterOrDigit(character) || character == '_' || character == '-') {
        sanitized.append(character);
      }
    }
    return sanitized.isEmpty() ? "field" : sanitized.toString();
  }

  private static String appendPath(String path, String token) {
    return path + "." + sanitizePathToken(token);
  }

  private static RuntimeConfigurationException invalid(String path, String problem) {
    return new RuntimeConfigurationException(path + " " + problem);
  }

  private enum EnvironmentValueType {
    STRING,
    INTEGER,
    LONG
  }

  private record EnvironmentOverride(
      String path, List<String> segments, EnvironmentValueType valueType) {
    private EnvironmentOverride {
      segments = List.copyOf(segments);
    }
  }

  private record EnvironmentResult(Object tree, int byteCount) {}
}
