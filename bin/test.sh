#!/usr/bin/env bash

# CD to the location of this executable
cd $(dirname "${BASH_SOURCE[0]}")
TEST_RESOURCES="../src/test/resources/"

function init () {

    # Check for confluent in PATH
    command -v confluent >/dev/null 2>&1 || {
       echo >&2 "confluent not available in PATH! exiting..."
       exit 1
    }

    echo "Clearing out confluent..."
    confluent destroy

    echo "Starting kafka connect (and dependencies)..."
    confluent start connect

    echo -n "Testing whether LLNLFileSourceConnector is available..."
    confluent list plugins | grep LLNLFileSourceConnector > /dev/null 2>&1
    if [ "$?" -eq 0 ]
    then
        echo "found!"
    else
        echo "not found!"
        echo "Aborting..."
        exit 1
    fi
}

PASSED=0
FAILED=0

function test_source () {
    NAME=$1
    TOPIC="${NAME}_topic"
    DATA_FILE="${TEST_RESOURCES}/${NAME}.json"
    PROPERTIES_FILE="${TEST_RESOURCES}/${NAME}.properties"

    echo "Loading test connector: $NAME"
    confluent load ${NAME} -d ${PROPERTIES_FILE}

    echo -n "Validating..."
    LINES=$(wc -l ${DATA_FILE})
    kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic ${TOPIC} \
        --from-beginning --max-messages ${LINES} --timeout-ms 5000 2> /dev/null | \
        diff ${DATA_FILE} - > /dev/null 2>&1

    if [ "$?" -eq 0 ]
    then
        echo "PASS"
        PASSED=$(expr ${PASSED} + 1)
    else
        echo "FAIL"
        FAILED=$(expr ${FAILED} + 1)
    fi
}

echo "========== INITIALIZING TEST STATE ==========="

init

echo "========== RUNNING TESTS ==========="

test_source "test_idstr"
test_source "test_alltypes"
test_source "test_idstr_dir"

echo "========== TEST RESULTS ==========="

echo "Tests passed: $PASSED"
echo "Tests failed: $FAILED"
