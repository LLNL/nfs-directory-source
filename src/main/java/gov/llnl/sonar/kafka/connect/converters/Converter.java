package gov.llnl.sonar.kafka.connect.converters;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class Converter<T> {
    abstract public Object convert(T from);

    public static Function<Object, Object> getConverterFor(AvroData avroData, Schema connectSchema, String format, JSONObject formatOptions) {
        switch (format) {
            case "json":
                return (Object o) -> avroData.toConnectData(connectSchema, o);
            case "csv":
                JSONArray arr = formatOptions.getJSONArray("columns");
                List<String> columns = new ArrayList<>();
                for(int i = 0; i < arr.length(); i++){
                    columns.add(arr.getString(i));
                }
                CsvRecordConverter csvRecordConverter = new CsvRecordConverter(connectSchema, columns);
                return (Object o) -> csvRecordConverter.convert((String[])o);
        }
        return null;
    }
}
