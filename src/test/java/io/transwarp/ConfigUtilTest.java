package io.transwarp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for ConfigUtil with default value support
 */
public class ConfigUtilTest {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtilTest.class);

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("ConfigUtil Default Value Test");
        System.out.println("========================================");
        
        try {
            // Test reading existing configuration
            String jdbcUrl = ConfigUtil.getString("jdbc.url");
            logger.info("PASS: Read existing config 'jdbc.url': {}", jdbcUrl.substring(0, Math.min(50, jdbcUrl.length())) + "...");
            
            // Test reading configuration with default value
            // Since analyze.level might not exist in config, test the default value mechanism
            try {
                String analyzeLevel = ConfigUtil.getString("analyze.level", "partition");
                logger.info("PASS: Read config 'analyze.level' with default: {}", analyzeLevel);
                
                // Verify default value
                if ("partition".equals(analyzeLevel) || "table".equals(analyzeLevel)) {
                    logger.info("PASS: analyze.level value is valid: {}", analyzeLevel);
                } else {
                    logger.warn("WARN: analyze.level has unexpected value: {}", analyzeLevel);
                }
            } catch (Exception e) {
                logger.error("FAIL: Error reading analyze.level: {}", e.getMessage());
            }
            
            // Test non-existent config with default value
            try {
                String nonExistent = ConfigUtil.getString("non.existent.key", "defaultValue");
                if ("defaultValue".equals(nonExistent)) {
                    logger.info("PASS: Default value mechanism works correctly");
                } else {
                    logger.error("FAIL: Expected 'defaultValue', got: {}", nonExistent);
                }
            } catch (Exception e) {
                logger.error("FAIL: Default value mechanism failed: {}", e.getMessage());
            }
            
            System.out.println("========================================");
            System.out.println("ConfigUtil test completed!");
            System.out.println("========================================");
        } catch (Exception e) {
            logger.error("Test failed with exception", e);
            System.exit(1);
        }
    }
}

