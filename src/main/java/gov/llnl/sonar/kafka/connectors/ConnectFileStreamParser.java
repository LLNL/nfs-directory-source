package gov.llnl.sonar.kafka.connectors;

import io.confluent.connect.avro.AvroData;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.EOFException;
import java.io.IOException;

@Slf4j
public abstract class ConnectFileStreamParser {

    AvroData avroConnectConverter;
    org.apache.kafka.connect.data.Schema connectSchema;

    public abstract Object read() throws EOFException;

    public abstract void close() throws IOException;

    ConnectFileStreamParser(Schema avroSchema) {

        this.avroConnectConverter = new AvroData(2);

        try {
            connectSchema = avroConnectConverter.toConnectSchema(avroSchema);
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

}
