package com.kongbig.sparkproject.test;

import com.kongbig.sparkproject.conf.ConfigurationManager;

/**
 * Describe:
 * Author:   kongbig
 * Data:     2018/1/30.
 */
public class ConfigurationManagerTest {

    public static void main(String[] args) {
        System.out.println(ConfigurationManager.getProperty("testkey1"));
    }
    
}
