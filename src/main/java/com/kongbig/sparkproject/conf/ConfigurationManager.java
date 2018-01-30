package com.kongbig.sparkproject.conf;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * Describe: 配置管理组件
 * Author:   kongbig
 * Data:     2018/1/30.
 */
public class ConfigurationManager {

    private static final Logger LOGGER = Logger.getLogger(ConfigurationManager.class);

    private static Properties prop = new Properties();

    /**
     * 静态代码块
     * Java中，每一个类每一次使用的时候，就会被Java虚拟机（JVM）中的类加载器，去磁盘上的.class文件中
     * 加载出来，然后为每个类都会构建一个Class对象，就代表了这个类
     */
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            // 通过Properties对象获取指定key对应的value
            prop.load(in);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

}
