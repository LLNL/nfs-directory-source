package gov.llnl.sonar.kafka.connect.connectors;

import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.*;

@Log4j
public class ConnectTestData {

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

    static Set<GenericData.Record> idstrAvroData = new HashSet<>(Arrays.asList(
            new GenericRecordBuilder(idstrAvroSchema).set("id", 1).set("str", "one").build(),
            new GenericRecordBuilder(idstrAvroSchema).set("id", 2).set("str", "two").build()
            ));

}
