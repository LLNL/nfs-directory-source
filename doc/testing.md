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

Now you should be able to run `bin/test.sh` (and press Ctrl+C to exit the test consumer).

Correct output looks like:

```

```