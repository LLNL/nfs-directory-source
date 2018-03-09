package gov.llnl.sonar.kafka.connect.util;

import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.Map;

public class ConnectUtils {
    public static Long getStreamOffset(SourceTaskContext context,
                                String PARTITION_FIELD,
                                String OFFSET_FIELD,
                                String partition) {

        Map<String, Object> offset = context.offsetStorageReader()
                .offset(Collections.singletonMap(PARTITION_FIELD, partition));

        Long streamOffset;

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
