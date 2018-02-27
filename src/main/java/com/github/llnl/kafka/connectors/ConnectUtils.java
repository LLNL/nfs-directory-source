package com.github.llnl.kafka.connectors;

import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.Map;

class ConnectUtils {
    static Long getStreamOffset(SourceTaskContext context,
                                String PARTITION_FIELD,
                                String OFFSET_FIELD,
                                String partition) {

        Map<String, Object> offset = context.offsetStorageReader()
                .offset(Collections.singletonMap(PARTITION_FIELD, partition));

        Long streamOffset = null;

        if (offset != null) {
            Object lastOffset = offset.get(OFFSET_FIELD);

            if (lastOffset != null && lastOffset instanceof Long) {
                streamOffset = (Long) lastOffset;
            } else {
                streamOffset = 0L;
            }
        } else {
            streamOffset = 0L;
        }

        return streamOffset;
    }

}
