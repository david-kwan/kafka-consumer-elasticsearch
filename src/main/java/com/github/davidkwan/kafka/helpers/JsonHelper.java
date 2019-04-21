package com.github.davidkwan.kafka.helpers;

import com.google.gson.JsonParser;

public class JsonHelper {

    private  static JsonParser jsonParser = new JsonParser();

    public static String getJsonPropertyValue(String json, String property) {
        return jsonParser.parse(json)
                .getAsJsonObject()
                .get(property)
                .getAsString();
    }
}
