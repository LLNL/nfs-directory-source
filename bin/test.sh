#!/usr/bin/env bash

if [ $(command -v confluent > /dev/null) ]
then
   echo "confluent not available in PATH! exiting..."
   exit 1
fi

# Clearing out confluent
confluent destroy

# Generate inputs
echo "
name=LLNLDirectorySourceConnector
tasks.max=1
connector.class=com.github.llnl.kafka.connectors.LLNLDirectorySourceConnector
filename=test.csv
topic=mytopic
" > mySourceConfig.properties

echo "1,2,3" > test.csv
echo "4,5,6" >> test.csv

# Start confluent and connectors

confluent start connect

#echo "Available connector plugins:"
confluent list plugins

echo "Loading LLNLDirectorySourceConnector..."
confluent load MySourceConnector -d mySourceConfig.properties

echo "Active connectors:"
confluent status connectors

echo "Status of LLNLDirectorySourceConnector:"
confluent status MySourceConnector

echo "Consuming from topic:"
kafka-console-consumer --bootstrap-server localhost:9092 --topic mytopic --from-beginning --max-messages 2 --timeout-ms 10

echo "If you saw rows 1,2,3 and 4,5,6, the test was a success!"

echo "Cleaning up..."

confluent destroy
rm mySourceConfig.properties test.csv