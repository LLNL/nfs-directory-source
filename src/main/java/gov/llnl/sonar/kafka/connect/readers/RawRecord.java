package gov.llnl.sonar.kafka.connect.readers;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.function.Function;

public class RawRecord {
    public Map sourcePartition;
    public Map sourceOffset;
    public Object rawData;

    RawRecord(Map sourcePartition, Map sourceOffset, Object rawData) {
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.rawData = rawData;
    }

    public SourceRecord toSourceRecord(
            String topic,
            Schema connectSchema,
            Function<Object, Object> converter) {

        Object parsedData = converter.apply(this.rawData);

        return new SourceRecord(
                this.sourcePartition,
                this.sourceOffset,
                topic, connectSchema, parsedData);
    }
}
