package gov.llnl.sonar.kafka.connect.readers;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class Reader {

    AtomicBoolean breakAndClose = new AtomicBoolean(false);

    public abstract Long read(List<SourceRecord> records, SourceTaskContext context);

    public void close() {
        breakAndClose.set(true);
    }

}
