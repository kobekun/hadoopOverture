package com.kobekun.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

public class GetPropertiesFromResourcesUtils {

    private static Properties properties = new Properties();

    static {

        try {

            properties.load(GetPropertiesFromResourcesUtils.class.getClassLoader()

            .getResourceAsStream("wc.properties"));
        } catch (IOException e) {

            e.printStackTrace();
        }
    }


    public static Properties getProperties(){

        return properties;
    }

    public static void main(String[] args) {

        System.out.println(getProperties().getProperty("OUTPUT_FILE"));
    }
}
