package myapps;

import org.json.JSONObject;

@FunctionalInterface
interface AnonymizationFunction {
    String execute(String inputString, JSONObject parameters);
}
