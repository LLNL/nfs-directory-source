package gov.llnl.sonar.kafka.connect.converters;

import org.apache.kafka.connect.data.Schema;

import java.text.MessageFormat;

public class ConvertException extends Exception {
    public ConvertException(Object val, Schema.Type type, String message) {
        super(MessageFormat.format(
                "Failed to convert value {0} to Kafka Connect type {1}\n{2}",
                val.toString(),
                type.getName(),
                message));
    }

    public ConvertException(Object val, String column, String message) {
        super(MessageFormat.format(
                "Failed to determine conversion type for column {0}, provided value {1}\n{2}",
                column,
                val.toString(),
                message));
    }
}

