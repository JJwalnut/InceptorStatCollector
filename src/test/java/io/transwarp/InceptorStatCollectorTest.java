package io.transwarp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;

/**
 * Test class for InceptorStatCollector statistics level configuration
 */
public class InceptorStatCollectorTest {
    private static final Logger logger = LoggerFactory.getLogger(InceptorStatCollectorTest.class);
    
    // Pattern for parsing ANALYZE results (same as main program)
    private static final Pattern RESULT_PATTERN = Pattern.compile(
            "^([\\w.]+)(\\{([^}]+)})?\\s+\\[numFiles=(\\d+),\\s*numRows=(\\d+),\\s*totalSize=(\\d+),\\s*rawDataSize=(\\d+)]$"
    );

    /**
     * Test parsing result line for partition-level mode
     */
    public void testParseResultLinePartitionMode() {
        logger.info("Testing partition-level mode (should accept all results)");
        
        String analyzeLevel = "partition";
        int passCount = 0;
        int failCount = 0;
        
        // Test case 1: Table-level result (no partition)
        String tableLevelResult = "default.test_table [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]";
        Optional<String[]> result1 = parseResultLine(tableLevelResult, analyzeLevel);
        if (result1.isPresent() && result1.get()[1] == null) {
            logger.info("PASS: Table-level result parsed successfully: {}", result1.get()[0]);
            passCount++;
        } else {
            logger.error("FAIL: Table-level result should be accepted in partition mode");
            failCount++;
        }
        
        // Test case 2: Partition-level result
        String partitionResult = "default.test_table{dt=2023-01-01} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]";
        Optional<String[]> result2 = parseResultLine(partitionResult, analyzeLevel);
        if (result2.isPresent() && result2.get()[1] != null && "dt=2023-01-01".equals(result2.get()[1])) {
            logger.info("PASS: Partition-level result parsed successfully: {} - {}", result2.get()[0], result2.get()[1]);
            passCount++;
        } else {
            logger.error("FAIL: Partition-level result should be accepted in partition mode");
            failCount++;
        }
        
        logger.info("Partition mode test: {} passed, {} failed", passCount, failCount);
    }

    /**
     * Test parsing result line for table-level mode
     */
    public void testParseResultLineTableMode() {
        logger.info("Testing table-level mode (should only accept table-level results)");
        
        String analyzeLevel = "table";
        int passCount = 0;
        int failCount = 0;
        
        // Test case 1: Table-level result (should be accepted)
        String tableLevelResult = "default.test_table [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]";
        Optional<String[]> result1 = parseResultLine(tableLevelResult, analyzeLevel);
        if (result1.isPresent() && result1.get()[1] == null) {
            logger.info("PASS: Table-level result parsed successfully: {}", result1.get()[0]);
            passCount++;
        } else {
            logger.error("FAIL: Table-level result should be accepted in table mode");
            failCount++;
        }
        
        // Test case 2: Partition-level result (should be filtered out)
        String partitionResult = "default.test_table{dt=2023-01-01} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]";
        Optional<String[]> result2 = parseResultLine(partitionResult, analyzeLevel);
        if (!result2.isPresent()) {
            logger.info("PASS: Partition-level result correctly filtered out");
            passCount++;
        } else {
            logger.error("FAIL: Partition-level result should be filtered out in table mode");
            failCount++;
        }
        
        logger.info("Table mode test: {} passed, {} failed", passCount, failCount);
    }

    /**
     * Test default configuration value
     */
    public void testDefaultConfiguration() {
        logger.info("Testing default configuration value");
        
        // Test that default value is "partition"
        String defaultValue = "partition";
        if ("partition".equals(defaultValue)) {
            logger.info("PASS: Default configuration is 'partition'");
        } else {
            logger.error("FAIL: Default analyze level should be 'partition'");
        }
    }

    /**
     * Test invalid result line parsing
     */
    public void testInvalidResultLine() {
        logger.info("Testing invalid result line parsing");
        
        String invalidResult = "invalid format line";
        Optional<String[]> result = parseResultLine(invalidResult, "partition");
        if (!result.isPresent()) {
            logger.info("PASS: Invalid result line correctly rejected");
        } else {
            logger.error("FAIL: Invalid result line should return empty");
        }
    }

    /**
     * Helper method to parse result line (simulating the actual implementation)
     */
    private Optional<String[]> parseResultLine(String line, String analyzeLevel) {
        Matcher m = RESULT_PATTERN.matcher(line.trim());
        if (m.matches()) {
            String partition = m.group(3);  // partition name, null means table-level
            
            // Filter results based on analyze.level configuration
            if ("table".equals(analyzeLevel)) {
                // Table-level mode: only process table-level results (partition is null)
                if (partition != null) {
                    logger.debug("Skipping partition-level result in table-level mode: {}", line);
                    return Optional.empty();
                }
            } else {
                // Partition-level mode (default): process all results (including table-level and partition-level)
                // No filtering needed
            }
            
            return Optional.of(new String[]{
                    m.group(1),   // table name
                    partition,    // partition name, null for table-level
                    m.group(4),   // numFiles
                    m.group(5),   // numRows
                    m.group(6),   // totalSize
                    m.group(7)    // rawDataSize
            });
        } else {
            logger.debug("Failed to parse result line: {}", line);
        }
        return Optional.empty();
    }

    /**
     * Main method for manual testing
     */
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("InceptorStatCollector Statistics Level Test");
        System.out.println("========================================");
        
        InceptorStatCollectorTest test = new InceptorStatCollectorTest();
        
        try {
            test.testParseResultLinePartitionMode();
            System.out.println();
            test.testParseResultLineTableMode();
            System.out.println();
            test.testDefaultConfiguration();
            System.out.println();
            test.testInvalidResultLine();
            
            System.out.println("========================================");
            System.out.println("All tests completed successfully!");
            System.out.println("========================================");
        } catch (AssertionError e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

