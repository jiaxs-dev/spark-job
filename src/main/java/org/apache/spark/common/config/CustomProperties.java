package org.apache.spark.common.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.common.utils.EmptyUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * 读取属性文件工具类
 *
 * @author guochengsen@dongao.com
 **/
@Slf4j
public class CustomProperties {

    /**
     * 自定义配置根目录
     */
    private static final String ROOT_PATH = "conf" + File.separator;

    private Properties properties;
    public static final CustomProperties CONFIG = new CustomProperties("config.properties");
    public static final CustomProperties JDBC = new CustomProperties("jdbc.properties");

    private CustomProperties(String confName) {
        this.properties = loadProp(ROOT_PATH + confName);
    }

    /**
     * 加载配置文件
     *
     * @param fileName 配置文件名(相对于classpath的文件地址)
     * @return
     */
    private Properties loadProp(String fileName) {
        if (EmptyUtils.isEmpty(fileName)) {
            throw new IllegalArgumentException("Properties file path can not be null.");
        }
        InputStream inputStream;
        Properties p = null;
        try {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
            p = new Properties();
            p.load(inputStream);
        } catch (Exception e) {
            log.error("load " + fileName + "failed.", e);
        }
        if (log.isDebugEnabled()) {
            try {
                Set<Entry<Object, Object>> entrySet = p.entrySet();
                log.debug("---------------" + fileName + "配置-------------------");
                entrySet.forEach(e -> {
                    log.debug(e.getKey() + "=" + e.getValue());
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return p;
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public Properties getProperties() {
        return properties;
    }
}
