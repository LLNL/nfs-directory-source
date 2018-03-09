package gov.llnl.sonar.kafka.connectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public interface Loggable {
    Class myClass = MethodHandles.lookup().lookupClass();
    Logger log = LoggerFactory.getLogger(myClass);
    String TAG = myClass.getName() + ": ";
}
