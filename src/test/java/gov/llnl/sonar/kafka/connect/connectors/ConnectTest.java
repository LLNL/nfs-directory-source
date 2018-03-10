package gov.llnl.sonar.kafka.connect.connectors;

import java.util.List;

public abstract class ConnectTest {

    protected ConfluentDriver confluent;

    abstract List<String> topics();
    abstract List<String> connectors();

    private void clearConnectData() {
        connectors().forEach(connector -> confluent.deleteConnector(connector));
        topics().forEach(topic -> confluent.deleteTopic(topic));
    }

    public void setup() {
        confluent = new ConfluentDriver("/Users/gimenez1/Home/local/src/confluent-4.0.0/bin");
        // clearConnectData();
        // confluent.command("start connect");
    }

    public void teardown() {
        clearConnectData();
        confluent.close();
    }
}
