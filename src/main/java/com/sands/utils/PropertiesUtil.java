package com.sands.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

public class PropertiesUtil {
    private final static ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("application");

    private final static String env = RESOURCE_BUNDLE.getString("profiles.active");

    private final static ResourceBundle ENV_RESOURCE_BUNDLE = ResourceBundle.getBundle("application-" + RESOURCE_BUNDLE.getString("profiles.active"));

    public static String getProperty(String key) {
        return ENV_RESOURCE_BUNDLE.getString(key);
    }

    private static String getProperty1(String key) {
        try (InputStream input = PropertiesUtil.class.getClassLoader().getResourceAsStream("application-" + env + ".properties")) {
            Properties prop = new Properties();
            prop.load(input);

            return prop.getProperty(key);

        } catch (Exception e) {

            return null;
        }
    }
}