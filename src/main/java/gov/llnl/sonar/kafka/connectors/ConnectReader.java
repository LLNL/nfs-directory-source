package gov.llnl.sonar.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class ConnectReader implements Loggable {

    AtomicBoolean breakAndClose = new AtomicBoolean(false);

    abstract Long read(List<SourceRecord> records, SourceTaskContext context);

    void close() {
        breakAndClose.set(true);
    }

}

