package com.github.llnl.kafka.connectors;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


public class SchemaUtils {
    public static org.apache.kafka.connect.data.Schema avroToKafkaSchema(org.apache.avro.Schema avroSchema) {

        Schema.Type type = avroSchema.getType();

        if (type == Schema.Type.RECORD) {

            SchemaBuilder builder = SchemaBuilder.struct();

            for (Schema.Field field : avroSchema.getFields()) {
                builder = builder.field(field.name(), avroToKafkaSchema(field.schema()));
            }

            return builder.build();

        } else {
            switch (avroSchema.getName()) {
                case ("boolean"):
                    return SchemaBuilder.bool();
                case ("bytes"):
                    return SchemaBuilder.bytes();
                case ("int"):
                    return SchemaBuilder.int32();
                case ("long"):
                    return SchemaBuilder.int64();
                case ("float"):
                    return SchemaBuilder.float32();
                case ("double"):
                    return SchemaBuilder.float64();
                case ("string"):
                    return SchemaBuilder.string();
            }
        }

        return null;
    }
}
