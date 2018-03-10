package gov.llnl.sonar.kafka.connect.connectors;

public abstract class ConnectTest {

    ConfluentDriver confluent;

    public void setup() {
        confluent = new ConfluentDriver("/Users/gimenez1/Home/local/src/confluent-4.0.0/bin");
    }

    public void teardown() {
        confluent.close();
    }
}
