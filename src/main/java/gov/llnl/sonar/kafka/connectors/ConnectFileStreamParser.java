package gov.llnl.sonar.kafka.connectors;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;

import java.io.EOFException;

public abstract class ConnectFileStreamParser implements Loggable {

    AvroData avroConnectConverter;
    org.apache.kafka.connect.data.Schema connectSchema;

    public abstract Object read() throws EOFException;

    ConnectFileStreamParser(Schema avroSchema) {

        this.avroConnectConverter = new AvroData(2);

        try {
            connectSchema = avroConnectConverter.toConnectSchema(avroSchema);
        } catch (Exception ex) {
            log.error(TAG, ex);
        }
    }
}
