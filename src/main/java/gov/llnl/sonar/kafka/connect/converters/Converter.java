package gov.llnl.sonar.kafka.connect.converters;

abstract class Converter<T> {
    abstract public Object convert(T from);
}
