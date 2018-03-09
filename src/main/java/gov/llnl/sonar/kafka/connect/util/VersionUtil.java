package gov.llnl.sonar.kafka.connect.util;

public class VersionUtil {
    public static String getVersion() {
        try {
            return VersionUtil.class.getPackage().getImplementationVersion();
        } catch(Exception ex){
            return "0.0.0.0";
        }
    }
}
