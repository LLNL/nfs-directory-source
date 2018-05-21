package gov.llnl.sonar.kafka.connect.util;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class OptionsParser {
    static public Map<String, String> optionsStringToMap(String options) {
        Map<String, String> optionsMap = new HashMap<>();

        JSONObject jsonObject = new JSONObject(options);
        Map<String, Object> jsonMap = jsonObject.toMap();

        for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
            optionsMap.put(jsonEntry.getKey(), jsonEntry.getValue().toString());
        }

        return optionsMap;
    }
}
