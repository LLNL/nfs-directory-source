package gov.llnl.sonar.kafka.connect.util;

import org.json.JSONObject;

import java.util.Map;

public class OptionsParser {
    static public Map<String, Object> optionsStringToMap(String options) {
        JSONObject jsonObject = new JSONObject(options);
        return jsonObject.toMap();
    }
}
