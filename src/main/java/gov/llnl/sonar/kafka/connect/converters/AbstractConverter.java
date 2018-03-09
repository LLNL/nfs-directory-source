package gov.llnl.sonar.kafka.connect.converters;

import org.apache.kafka.common.record.Record;

abstract class AbstractConverter<T> {
    abstract Record convert(T from);
}
