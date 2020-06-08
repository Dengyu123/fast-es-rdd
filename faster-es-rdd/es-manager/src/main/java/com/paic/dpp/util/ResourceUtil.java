package com.paic.dpp.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
public class ResourceUtil {

    private ResourceUtil(){}
    private static Properties properties = new Properties();

    static {
        ResourceUtil resourceUtil = new ResourceUtil();
        InputStream resourceAsStream = resourceUtil.getClass().getClassLoader().getResourceAsStream("fer.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Properties getProperties() {
        return properties;
    }

}
