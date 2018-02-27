package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public abstract class ConnectReader {

    private Long batchSize;

    abstract Long read(List<SourceRecord> records, String streamPartition, Long streamOffset);
    abstract void close();

}

