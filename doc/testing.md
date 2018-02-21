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

to include the absolute location of the packaged connectors jar file we built in `./target`:

```
plugin.path=share/java,~/src/kafkaconnectors/target
```

Now you should be able to run `bin/test.sh`

Correct output looks like:

```
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
Deleting: /var/folders/p8/vbk8nbcd5n594hgbc89xpnh4001_x9/T/confluent.VJypaxBK
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
    "class": "com.github.llnl.kafka.connectors.LLNLDirectorySourceConnector",
    "type": "source",
    "version": "null"
  },
  {
    "class": "com.github.llnl.kafka.connectors.LLNLDirectorySourceConnector",
    "type": "source",
    "version": "1.0-SNAPSHOT"
  },
  {
    "class": "com.github.llnl.kafka.connectors.MySinkConnector",
    "type": "sink",
    "version": "null"
  },
  {
    "class": "com.github.llnl.kafka.connectors.MySinkConnector",
    "type": "sink",
    "version": "1.0-SNAPSHOT"
  },
  {
    "class": "com.github.llnl.kafka.connectors.MySourceConnector",
    "type": "source",
    "version": "null"
  },
  {
    "class": "com.github.llnl.kafka.connectors.MySourceConnector",
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
Loading LLNLDirectorySourceConnector...
{
  "name": "MySourceConnector",
  "config": {
    "tasks.max": "1",
    "connector.class": "com.github.llnl.kafka.connectors.LLNLDirectorySourceConnector",
    "filename": "test.csv",
    "topic": "mytopic",
    "name": "MySourceConnector"
  },
  "tasks": [],
  "type": null
}
Active connectors:
{
  "error_code": 409,
  "message": "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config change)"
}
Status of LLNLDirectorySourceConnector:
{
  "name": "MySourceConnector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "10.244.250.239:8083"
  },
  "tasks": [],
  "type": "source"
}
Consuming from topic:

1,2,3

4,5,6
Processed a total of 2 messages
If you saw rows 1,2,3 and 4,5,6, the test was a success!
Cleaning up...
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
Deleting: /var/folders/p8/vbk8nbcd5n594hgbc89xpnh4001_x9/T/confluent.rwDugQAr
```