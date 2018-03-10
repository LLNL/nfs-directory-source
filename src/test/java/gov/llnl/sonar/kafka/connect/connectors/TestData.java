package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

@Log4j
public class TestData {

    static String idstrAvroSchemaString = "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"idstr\"," +
            "  \"connect.name\": \"idstr\"," +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"id\"," +
            "      \"type\": \"int\"" +
            "    }," +
            "    {" +
            "      \"name\": \"str\"," +
            "      \"type\": \"string\"" +
            "    }" +
            "  ]" +
            "}";

    static String idstrAvroSchemaEscapedString = idstrAvroSchemaString.replaceAll("\"", "\\\"");

    static Schema idstrAvroSchema = new Schema.Parser().parse(idstrAvroSchemaString);

    static List<String> testRecords = Arrays.asList(
            "{\"id\": 1, \"str\": \"one\"}",
            "{\"id\": 2, \"str\": \"two\"}"
    );

    static void validateTopicContents(ConfluentDriver confluent, String topic) {

        log.info("Consuming topic " + topic);

        Consumer consumer = confluent.createConsumer(topic);
        Iterable<ConsumerRecord> consumerStream = consumer.poll(10000);

        List<String> consumedRecords = new ArrayList<>();
        for (ConsumerRecord consumerRecord : consumerStream) {
            GenericData.Record record = (GenericData.Record) consumerRecord.value();
            log.info("<<< Consumed record: " + record.toString());
            consumedRecords.add(record.toString());
            assertEquals(idstrAvroSchema, record.getSchema());
        }

        consumer.close();

        assertEquals(testRecords, consumedRecords);
    }


}
