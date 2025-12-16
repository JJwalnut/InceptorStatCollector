package io.transwarp;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    private static final Properties props = new Properties();

    static {
        try {
            // 优先从 conf/config.properties 加载（推荐方式）
            File externalConfig = new File("conf/config.properties");
            if (externalConfig.exists() && externalConfig.isFile()) {
                try (FileInputStream fis = new FileInputStream(externalConfig);
                     InputStreamReader reader = new InputStreamReader(fis, StandardCharsets.UTF_8)) {
                    props.load(reader);
                    logger.info("Loaded configuration from external file: {}", externalConfig.getAbsolutePath());
                }
            } else {
                // 如果外部配置文件不存在，从jar包内部加载
                try (InputStream is = ConfigUtil.class.getClassLoader()
                        .getResourceAsStream("config.properties")) {
                    if (is == null) {
                        throw new RuntimeException("Configuration file not found, please ensure config.properties exists in conf/config.properties or inside jar");
                    }
                    try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                        props.load(reader);
                    }
                    logger.info("Loaded configuration from jar internal");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration file", e);
        }
    }

    // 获取字符串配置
    public static String getString(String key) {
        String value = props.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Missing required configuration item: " + key);
        }
        return value.trim();
    }

    // 获取字符串配置（带默认值）
    public static String getString(String key, String defaultValue) {
        String value = props.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value.trim();
    }

    // 获取整数配置
    public static int getInt(String key) {
        try {
            return Integer.parseInt(getString(key));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Configuration item format error: " + key, e);
        }
    }

    // 获取整数配置（带默认值）
    public static int getInt(String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Configuration item format error: {}, using default value: {}", key, defaultValue);
            return defaultValue;
        }
    }
}