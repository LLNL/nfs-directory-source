#!/usr/bin/env bash

if [ $(command -v confluent > /dev/null) ]
then
   echo "confluent not available in PATH! exiting..."
   exit 1
fi

# Clearing out confluent
confluent destroy

# Generate inputs
echo '
name=LLNLFileSourceConnector
tasks.max=1
connector.class=com.github.llnl.kafka.connectors.LLNLFileSourceConnector
filename=test.csv
format=json
format.options=
topic=mytopic
avro.schema={\"type\":\"record\",\"name\":\"idstr\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"str\",\"type\":\"string\"}]}
' > mySourceConfig.properties

echo '{ "id" : 1, "str" : "one" }' > test.csv
echo '{ "id" : 2, "str" : "two" }' >> test.csv

# Start confluent and connectors

confluent start connect

#echo "Available connector plugins:"
confluent list plugins

echo "Loading LLNLFileSourceConnector..."
confluent load LLNLFileSourceConnector -d mySourceConfig.properties

echo "Active connectors:"
confluent status connectors

echo "Status of LLNLFileSourceConnector:"
confluent status LLNLFileSourceConnector

echo "Consuming from topic:"
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic mytopic --from-beginning --max-messages 2 --timeout-ms 10

echo "If you saw rows 1,2,3 and 4,5,6, the test was a success!"

echo "Cleaning up..."

#confluent destroy
rm mySourceConfig.properties test.csv