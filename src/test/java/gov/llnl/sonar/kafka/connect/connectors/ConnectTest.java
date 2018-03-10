package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

@Log4j
public abstract class ConnectTest {

    ConfluentDriver confluent;

    public void setup() {
        confluent = new ConfluentDriver("/Users/gimenez1/Home/local/src/confluent-4.0.0/bin");
    }

    public void teardown() {
        confluent.close();
    }

    void validateTopicContents(String topic, Set<GenericData.Record> trueData) {

        log.info("Consuming topic " + topic);

        Set<GenericData.Record> consumedRecords = new HashSet<>();
        Consumer consumer = confluent.createConsumer(topic);
        Iterable<ConsumerRecord> consumerStream = consumer.poll(30000);

        for (ConsumerRecord consumerRecord : consumerStream) {

            // Parse to avro record
            GenericData.Record record = (GenericData.Record) consumerRecord.value();
            log.info("<<< Consumed record: " + record.toString());

            consumedRecords.add(record);

        }

        consumer.close();

        assertEquals(trueData, consumedRecords);
    }


}
