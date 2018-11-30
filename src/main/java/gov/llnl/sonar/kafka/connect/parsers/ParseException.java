package gov.llnl.sonar.kafka.connect.parsers;

import java.text.MessageFormat;

public class ParseException extends Exception {
    ParseException(FileStreamParser fileStreamParser, String message) {
        super(MessageFormat.format(
                "Parse exception at file {0}: offset {1}\n{2}",
                fileStreamParser.fileName,
                fileStreamParser.offset.toString(),
                message));
    }
}
