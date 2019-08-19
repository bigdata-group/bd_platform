package com.bigdata.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jiwenlong on 2017/4/26.
 */
public class PropertiesReader {
    private static Properties bdConfProps = null;

    public static Properties getBigDataConf() {
        synchronized (PropertiesReader.class) {
            if (bdConfProps == null) {
                bdConfProps = readProperties("bdConf.properties");
            }
        }
        return bdConfProps;
    }

    /**
     * 不要频繁调用，最好写个单例
     * @param propFile
     * @return
     */
    public static Properties readProperties(String propFile) {
        Properties props = new Properties();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(propFile)) {
            props.load(is);
        } catch (IOException e){

        }
        return props;
    }
}

