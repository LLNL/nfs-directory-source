package com.github.llnl.kafka.connectors;

class SchemaUtils {
    static org.apache.kafka.connect.data.Schema avroToKafkaConnectSchema(org.apache.avro.Schema avroSchema) {

        org.apache.avro.Schema.Type type = avroSchema.getType();

        if (type == org.apache.avro.Schema.Type.RECORD) {

            org.apache.kafka.connect.data.SchemaBuilder builder = org.apache.kafka.connect.data.SchemaBuilder.struct();

            for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                builder = builder.field(field.name(), avroToKafkaConnectSchema(field.schema()));
            }

            return builder.build();

        } else {
            switch (avroSchema.getName()) {
                case ("boolean"):
                    return org.apache.kafka.connect.data.SchemaBuilder.bool();
                case ("bytes"):
                    return org.apache.kafka.connect.data.SchemaBuilder.bytes();
                case ("int"):
                    return org.apache.kafka.connect.data.SchemaBuilder.int32();
                case ("long"):
                    return org.apache.kafka.connect.data.SchemaBuilder.int64();
                case ("float"):
                    return org.apache.kafka.connect.data.SchemaBuilder.float32();
                case ("double"):
                    return org.apache.kafka.connect.data.SchemaBuilder.float64();
                case ("string"):
                    return org.apache.kafka.connect.data.SchemaBuilder.string();
            }
        }

        throw new UnsupportedOperationException(String.format("Cannot convert avro schema %s to Kafka Connect schema", avroSchema));
    }

    static org.apache.kafka.connect.data.Struct
    genericDataRecordToKafkaConnectStruct(org.apache.avro.generic.GenericData.Record record,
                                          org.apache.kafka.connect.data.Schema connect_schema) {

        org.apache.kafka.connect.data.Struct struct = new org.apache.kafka.connect.data.Struct(connect_schema);

        for (org.apache.kafka.connect.data.Field field : connect_schema.fields()) {
            String name = field.name();
            Object value = record.get(field.name());

            // Kafka connect wants String not Utf8
            if (value instanceof org.apache.avro.util.Utf8) {
                value = value.toString();
            }

            struct = struct.put(name, value);
        }

        return struct;
    }
}
