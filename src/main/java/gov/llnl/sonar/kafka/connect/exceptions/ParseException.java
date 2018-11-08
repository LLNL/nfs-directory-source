package gov.llnl.sonar.kafka.connect.exceptions;

public class ParseException extends RuntimeException {
    public ParseException() { super(); }
    public ParseException(String message) { super(message); }
}
