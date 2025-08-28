package com.sands.realtime.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.ResourceBundle;

public class PropertiesUtil {
    private final static ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("application");

    private final static ResourceBundle ENV_RESOURCE_BUNDLE = ResourceBundle.getBundle("application-" + RESOURCE_BUNDLE.getString("profiles.active"));

    public static String getProperty(String key) {
        return ENV_RESOURCE_BUNDLE.getString(key);
    }

    public static ParameterTool getPropertiesParameters () throws IOException {
        return ParameterTool.fromPropertiesFile("realtime-common/src/main/resources/application-" + RESOURCE_BUNDLE.getString("profiles.active") + ".properties");
    }
}