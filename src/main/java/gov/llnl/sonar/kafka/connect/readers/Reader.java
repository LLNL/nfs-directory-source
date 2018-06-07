package gov.llnl.sonar.kafka.connect.readers;

import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class Reader {

    public abstract Long read(List<RawRecord> rawRecords, SourceTaskContext context);

}
