# Testing

Install confluent and add it to PATH, e.g. `PATH=~/confluent-4.0.0/bin:$PATH`

Add the packaged connectors to confluent's schema-registry properties, e.g.:

In files:
```
confluent-4.0.0/etc/schema-registry/connect-avro-standalone.properties
confluent-4.0.0/etc/schema-registry/connect-avro-standalone.properties
```

Modify the last line from the default:

```
plugin.path=share/java
```

...to include the absolute location of the packaged connectors jar file we built in `./target`:

```
plugin.path=share/java,~/src/kafkaconnectors/target
```

Now you should be able to run `bin/test.sh`

Correct output looks like:

```
Clearing out confluent...
Stopping connect
connect is [DOWN]
Stopping kafka-rest
kafka-rest is [DOWN]
Stopping schema-registry
schema-registry is [DOWN]
Stopping kafka
kafka is [DOWN]
Stopping zookeeper
zookeeper is [DOWN]
Deleting: /var/folders/p8/vbk8nbcd5n594hgbc89xpnh4001_x9/T/confluent.uuFF7auS
Starting kafka connect (and dependencies)...
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
Available Connector Plugins: 
[
  {
    "class": "com.github.llnl.kafka.connectors.LLNLFileSourceConnector",
    "type": "source",
    "version": "null"
  },
  {
    "class": "com.github.llnl.kafka.connectors.LLNLFileSourceConnector",
    "type": "source",
    "version": "1.0-SNAPSHOT"
  },
  {
    "class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "type": "sink",
    "version": "4.0.0"
  },
  {
    "class": "io.confluent.connect.hdfs.HdfsSinkConnector",
    "type": "sink",
    "version": "4.0.0"
  },
  {
    "class": "io.confluent.connect.hdfs.tools.SchemaSourceConnector",
    "type": "source",
    "version": "1.0.0-cp1"
  },
  {
    "class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "type": "sink",
    "version": "4.0.0"
  },
  {
    "class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "type": "source",
    "version": "4.0.0"
  },
  {
    "class": "io.confluent.connect.s3.S3SinkConnector",
    "type": "sink",
    "version": "4.0.0"
  },
  {
    "class": "io.confluent.connect.storage.tools.SchemaSourceConnector",
    "type": "source",
    "version": "1.0.0-cp1"
  },
  {
    "class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "type": "sink",
    "version": "1.0.0-cp1"
  },
  {
    "class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "type": "source",
    "version": "1.0.0-cp1"
  }
]
Loading LLNLFileSourceConnector...
{
  "name": "LLNLFileSourceConnector",
  "config": {
    "tasks.max": "1",
    "connector.class": "com.github.llnl.kafka.connectors.LLNLFileSourceConnector",
    "filename": "../src/test/resources/test_idstr.json",
    "format": "json",
    "format.options": "",
    "topic": "mytopic",
    "avro.schema": "{\"type\":\"record\",\"name\":\"idstr\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"str\",\"type\":\"string\"}]}",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "name": "LLNLFileSourceConnector"
  },
  "tasks": [],
  "type": null
}
Status of LLNLFileSourceConnector:
{
  "name": "LLNLFileSourceConnector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "10.247.240.121:8083"
  },
  "tasks": [],
  "type": "source"
}
Sleeping for a second...
test "idstr": Processed a total of 2 messages
PASS
Tests passed: 1
Tests failed: 0
```