package gov.llnl.sonar.kafka.connect.converters;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Arrays;

@Slf4j
public class CsvRecordConverter {

    private final Schema connectSchema;
    private final String[] columns;

    public CsvRecordConverter(Schema schema, String[] columns) {
        this.connectSchema = schema;
        this.columns = columns;
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

    public Struct convert(String[] csvTokens) {
        Struct record = new Struct(connectSchema);

        int i = 0;
        for (String column : columns) {

            String value = csvTokens[i++];

            try {
                Object parsedValue = stringToConnectObject(value, connectSchema.field(column).schema().type());
                record = record.put(column, parsedValue);
            } catch (NumberFormatException e) {
                log.error("Failed to parse column {}, value {}, expected type {}",
                        column, value,
                        connectSchema.field(column).schema().type(), e);
            } catch (NullPointerException e) {
                log.error("Failed to get schema for column {}", column);
                log.error("Schema contents: " +
                        Arrays.toString(connectSchema.fields().stream().map(
                                f -> f.name() + " : " + f.schema().name()).toArray()
                        )
                );
            }
        }

        return record;
    }
}
