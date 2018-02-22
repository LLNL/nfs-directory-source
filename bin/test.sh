#!/usr/bin/env bash

command -v confluent >/dev/null 2>&1 || {
   echo >&2 "confluent not available in PATH! exiting..."
   exit 1
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ${DIR}

echo "Clearing out confluent..."
confluent destroy

echo "Starting kafka connect (and dependencies)..."
confluent start connect

confluent list plugins

echo "Loading LLNLFileSourceConnector..."
confluent load LLNLFileSourceConnector -d ../src/test/resources/test_idstr.properties

echo "Status of LLNLFileSourceConnector:"
confluent status LLNLFileSourceConnector

echo "Sleeping for a second..."
sleep 1

PASSED=0
FAILED=0

echo -n "test \"idstr\": "
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic mytopic \
    --from-beginning --max-messages 2 --timeout-ms 10 | diff ../src/test/resources/test_idstr.json -
if [ "$?" -eq 0 ]
then
    echo "PASS"
    PASSED=$(expr ${PASSED} + 1)
else
    echo "FAIL"
    FAILED=$(expr ${FAILED} + 1)
fi

echo "Tests passed: $PASSED"
echo "Tests failed: $FAILED"
