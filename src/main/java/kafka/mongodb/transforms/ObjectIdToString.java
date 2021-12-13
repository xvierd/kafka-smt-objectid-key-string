package kafka.mongodb.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ObjectIdToString<R extends ConnectRecord<R>> implements Transformation<R> {
  public static String FIELD_NAME = "field.name";
  private static final String PURPOSE = "transform mongoObjectId to string";
  static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
  private String fieldName;

  @SuppressWarnings("serial")
  private static final Map<Schema.Type, Schema> TYPES_MAP = new HashMap<Schema.Type, Schema>() {{
    put(Schema.Type.INT8, Schema.INT8_SCHEMA);
    put(Schema.Type.INT16, Schema.INT16_SCHEMA);
    put(Schema.Type.INT32, Schema.INT32_SCHEMA);
    put(Schema.Type.INT64, Schema.INT64_SCHEMA);
    put(Schema.Type.FLOAT32, Schema.FLOAT32_SCHEMA);
    put(Schema.Type.FLOAT64, Schema.FLOAT64_SCHEMA);
    put(Schema.Type.BOOLEAN, Schema.BOOLEAN_SCHEMA);
    put(Schema.Type.STRING, Schema.STRING_SCHEMA);
    put(Schema.Type.BYTES, Schema.BYTES_SCHEMA);
  }};

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    }

    return applyWithSchema(record);
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);

    if (value == null) {
      return record;
    }

    String valueString = value.get(fieldName).toString();

    if (!valueString.contains("$oid")) {
      return record;
    }

    final HashMap<String, Object> updatedValue = new HashMap<>(value);

    updatedValue.put("key", objectIdToString(value.get(fieldName)));

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

    if (value == null) {
      return record;
    }

    Schema schema = operatingSchema(record);

    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(
        schema,
        SchemaBuilder.struct()
    );

    for (Field field : value.schema().fields()) {
      if (field.name().equals(fieldName)) {
        builder.field("key", Schema.STRING_SCHEMA);
      } else {
        builder.field(field.name(), TYPES_MAP.get(field.schema().type()));
      }
    }

    Schema newSchema = builder.build();
    final Struct newValue = new Struct(newSchema);

    for (Field field : value.schema().fields()) {
      if (field.name().equals(fieldName)) {
        newValue.put("key", objectIdToString(value.get(fieldName)));
      } else {
        newValue.put(field.name(), value.get(field));
      }
    }

    return newRecord(record, newSchema, newValue);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    fieldName = config.getString(FIELD_NAME);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  private String objectIdToString(Object input) {

    String value = input.toString();
    value = value.substring(1, value.length()-1);           //remove curly brackets
    String[] keyValuePairs = value.split(",");              //split the string to creat key-value pairs
    Map<String,String> map = new HashMap<>();

    for(String pair : keyValuePairs)                        //iterate over the pairs
    {
      String[] entry = pair.split("=");                   //split the pairs to get key and value
      map.put(entry[0].trim(), entry[1].trim());          //add them to the hashmap and trim whitespaces
    }

    return map.get("$oid");
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Value<R extends ConnectRecord<R>> extends ObjectIdToString<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
}
