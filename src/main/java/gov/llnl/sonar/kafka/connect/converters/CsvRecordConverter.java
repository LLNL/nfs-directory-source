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
public class CsvRecordConverter extends Converter<String[]>{

    private final Schema connectSchema;
    private final List<String> header;

    public CsvRecordConverter(Schema schema, List<String> header) {
        this.connectSchema = schema;
        this.header = header;
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
    public Object convert(String[] csvTokens) {
        Struct record = new Struct(connectSchema);

        int i = 0;
        for (String column : header) {

            String value = csvTokens[i++];

            try {
                Object parsedValue = stringToConnectObject(value, connectSchema.field(column).schema().type());
                record = record.put(column, parsedValue);
            } catch (NumberFormatException e) {
                log.error("Failed to parse column {}, value {}, expected type {}",
                        column, value,
                        connectSchema.field(column).schema().type(), e);
            }
        }

        return record;
    }
}
