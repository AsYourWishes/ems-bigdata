package com.thtf.bigdata.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    public static Properties prop;
    public static String getPropertiesByKey(String key){
        return prop.getProperty(key);
    }
    static{
        if (prop == null) {
        	prop = new Properties();
            InputStream in = PropertiesUtils.class.getResourceAsStream("/baseconfiguration.properties");
            try {
            	prop.load(in);
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                System.out.println("read urlname from config error!!! ");
            }
        }
    }
}
