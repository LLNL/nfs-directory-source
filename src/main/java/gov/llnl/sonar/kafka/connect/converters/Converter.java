package gov.llnl.sonar.kafka.connect.converters;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.function.Function;

public abstract class Converter<T> {
    abstract public Object convert(T from);

    public static Function<Object, Object> getConverterFor(AvroData avroData, Schema connectSchema, String format) {
        switch (format) {
            case "json":
                return (Object o) -> avroData.toConnectData(connectSchema, o);
            case "csv":
                CsvRecordConverter csvRecordConverter = new CsvRecordConverter(connectSchema);
                return (Object o) -> csvRecordConverter.convert((Map<String, String>)o);
        }
        return null;
    }
}
