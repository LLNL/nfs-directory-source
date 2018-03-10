package gov.llnl.sonar.kafka.connect.connectors;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

@Log4j
public class ConfluentDriver {

    Client restClient;
    private WebTarget target;
    private String confluentDir;

    private String cmdConfluent;
    private String cmdKafkaTopics;

    ConfluentDriver(String confluentDir) {

        this.confluentDir = confluentDir;
        this.cmdConfluent = confluentDir + "/confluent";
        this.cmdKafkaTopics = confluentDir + "/kafka-topics";

        command(cmdConfluent + " start connect");

        connectToRest();
    }

    private void connectToRest() {
        restClient = ClientBuilder.newClient();
        target = restClient.target("http://localhost:8083");
    }


    private String[] commandWithBash(String command) {
        return new String[] {
                "/usr/bin/env bash",
                "-c",
                command
        };
    }

    void command(String command) {
        command(command.split("\\s+"));
    }

    private void command(String[] command) {

        ProcessBuilder builder = new ProcessBuilder().command(command);

        try {
            Process proc = builder.start();

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(proc.getErrorStream()));

            String s = null;
            while ((s = stdInput.readLine()) != null) {
                log.info(s);
            }

            while ((s = stdError.readLine()) != null) {
                log.error(s);
            }

        } catch (IOException ex) {
            log.error("Confluent command failed");
        }

    }

    public void deleteTopic(String topic) {
        command(cmdKafkaTopics + " --zookeeper localhost:2181 --delete --topic " + topic);
    }

    public String connectorStatus() {
        return target.path("connectors")
                .request()
                .get(String.class);
    }

    public String createConnector(String name, Class connectorClass, Map<String, String> configProperties) {

        configProperties.put("connector.class", connectorClass.getName());

        String jsonConfigString = new JSONObject(configProperties).toString();
        String jsonConnectorString = "{\"name\": \"" + name + "\", \"config\": " + jsonConfigString + "}";

        return createConnector(jsonConnectorString);
    }

    public String createConnector(String connector) {
        Response response = target.path("connectors")
                .request()
                .post(Entity.entity(connector, MediaType.APPLICATION_JSON_TYPE));

        return response.readEntity(String.class);
    }

    public String deleteConnector(String connector) {
        Response response = target.path("connectors/" + connector)
                .request()
                .delete();

        return response.readEntity(String.class);
    }

    public Consumer createConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081", 2);
        Deserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);

        // Create the consumer using props.
        Consumer consumer = new KafkaConsumer(props, deserializer, deserializer);
        consumer.subscribe(Collections.singletonList(topic));
        consumer.seekToBeginning(consumer.assignment());

        return consumer;
    }

    public void close() {
        restClient.close();
    }

}
