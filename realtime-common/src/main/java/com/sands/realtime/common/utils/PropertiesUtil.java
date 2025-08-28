package com.sands.realtime.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ResourceBundle;

public class PropertiesUtil {
    private final static ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("application");

    public static String getProperty(String key) {
        String activeProfile = RESOURCE_BUNDLE.getString("profiles.active");
        ResourceBundle envBundle = ResourceBundle.getBundle("application-" + activeProfile);
        return envBundle.getString(key);
    }

    public static ParameterTool getPropertiesParameters() throws IOException {
        String activeProfile = RESOURCE_BUNDLE.getString("profiles.active");
        String propertiesFile = "application-" + activeProfile + ".properties";

        // 使用类加载器读取相对路径
        InputStream inputStream = PropertiesUtil.class.getClassLoader()
                .getResourceAsStream(propertiesFile);

        if (inputStream == null) {
            throw new IOException("Properties file not found: " + propertiesFile);
        }

        return ParameterTool.fromPropertiesFile(inputStream);
    }
}