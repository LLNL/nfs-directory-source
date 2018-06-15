package gov.llnl.sonar.kafka.connect.converters;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CsvRecordConverter extends Converter<Map<String, String>>{

    private final Schema connectSchema;

    public CsvRecordConverter(Schema schema) {
        connectSchema = schema;
    }

    public Object stringToConnectObject(String s, Schema.Type type) {

        switch (type) {
            case STRING:
                return s;
            case INT8:
                return Byte.valueOf(s);
            case INT16:
                return Short.valueOf(s);
            case INT32:
                return Integer.valueOf(s);
            case INT64:
                return Long.valueOf(s);
            case FLOAT32:
                return Float.valueOf(s);
            case FLOAT64:
                return Double.valueOf(s);
            case BOOLEAN:
                return Boolean.valueOf(s);
            case BYTES:
                return ByteBuffer.wrap(s.getBytes());
            case STRUCT:
            case MAP:
            case ARRAY:
                throw new DataException("Non-primitive types not supported for CSV file sources!");
        }

        return null;
    }

    @Override
    public Object convert(Map<String, String> recordMap) {
        Struct record = new Struct(connectSchema);

        for (Map.Entry<String, String> entry : recordMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            List<String> fieldNames = connectSchema.fields().stream()
                    .map((Field f) -> f.name())
                    .collect(Collectors.toList());

            if (!fieldNames.contains(key)) {
                log.error("Requested field: {}", key);
                log.error("Value: {}", value);

                List<String> fields = connectSchema.fields().stream()
                        .map((Field f) -> "{" + f.name() + ":" + f.schema().type().getName() + "}")
                        .collect(Collectors.toList());

                log.error("Available fields: {}", String.join(",", fields));
                throw new DataException("Schema mismatch");
            }

            try {
                Object parsedValue = stringToConnectObject(value, connectSchema.field(key).schema().type());
                record = record.put(key, parsedValue);
            } catch (NumberFormatException e) {
                log.error("Failed to parse key {}, value {}, expected type {}", key, value, connectSchema.field(key).schema().type(), e);
            }
        }

        return record;
    }
}
