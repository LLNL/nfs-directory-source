package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;

abstract class ConnectReader {

    abstract Long read(List<SourceRecord> records, SourceTaskContext context);
    abstract void close();

}

