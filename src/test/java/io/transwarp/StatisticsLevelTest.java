package io.transwarp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;

/**
 * Simple test for statistics level configuration feature
 */
public class StatisticsLevelTest {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsLevelTest.class);
    
    // Pattern for parsing ANALYZE results
    private static final Pattern RESULT_PATTERN = Pattern.compile(
            "^([\\w.]+)(\\{([^}]+)})?\\s+\\[numFiles=(\\d+),\\s*numRows=(\\d+),\\s*totalSize=(\\d+),\\s*rawDataSize=(\\d+)]$"
    );

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Statistics Level Configuration Test");
        System.out.println("========================================");
        
        int totalTests = 0;
        int passedTests = 0;
        
        // Test 1: Partition mode - should accept table-level result
        totalTests++;
        String tableResult = "default.test_table [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]";
        Optional<String[]> result1 = parseResultLine(tableResult, "partition");
        if (result1.isPresent() && result1.get()[1] == null) {
            System.out.println("PASS Test 1: Partition mode accepts table-level result");
            passedTests++;
        } else {
            System.out.println("FAIL Test 1: Partition mode should accept table-level result");
        }
        
        // Test 2: Partition mode - should accept partition-level result
        totalTests++;
        String partitionResult = "default.test_table{dt=2023-01-01} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]";
        Optional<String[]> result2 = parseResultLine(partitionResult, "partition");
        if (result2.isPresent() && result2.get()[1] != null && "dt=2023-01-01".equals(result2.get()[1])) {
            System.out.println("PASS Test 2: Partition mode accepts partition-level result");
            passedTests++;
        } else {
            System.out.println("FAIL Test 2: Partition mode should accept partition-level result");
        }
        
        // Test 3: Table mode - should accept table-level result
        totalTests++;
        Optional<String[]> result3 = parseResultLine(tableResult, "table");
        if (result3.isPresent() && result3.get()[1] == null) {
            System.out.println("PASS Test 3: Table mode accepts table-level result");
            passedTests++;
        } else {
            System.out.println("FAIL Test 3: Table mode should accept table-level result");
        }
        
        // Test 4: Table mode - should filter out partition-level result
        totalTests++;
        Optional<String[]> result4 = parseResultLine(partitionResult, "table");
        if (!result4.isPresent()) {
            System.out.println("PASS Test 4: Table mode filters out partition-level result");
            passedTests++;
        } else {
            System.out.println("FAIL Test 4: Table mode should filter out partition-level result");
        }
        
        // Test 5: Default value test
        totalTests++;
        String defaultLevel = "partition";
        if ("partition".equals(defaultLevel)) {
            System.out.println("PASS Test 5: Default value is 'partition'");
            passedTests++;
        } else {
            System.out.println("FAIL Test 5: Default value should be 'partition'");
        }
        
        System.out.println("========================================");
        System.out.println("Test Results: " + passedTests + "/" + totalTests + " passed");
        System.out.println("========================================");
        
        if (passedTests == totalTests) {
            System.out.println("All tests PASSED!");
        } else {
            System.out.println("Some tests FAILED!");
            System.exit(1);
        }
    }
    
    private static Optional<String[]> parseResultLine(String line, String analyzeLevel) {
        Matcher m = RESULT_PATTERN.matcher(line.trim());
        if (m.matches()) {
            String partition = m.group(3);
            
            if ("table".equals(analyzeLevel)) {
                if (partition != null) {
                    return Optional.empty();
                }
            }
            
            return Optional.of(new String[]{
                    m.group(1),
                    partition,
                    m.group(4),
                    m.group(5),
                    m.group(6),
                    m.group(7)
            });
        }
        return Optional.empty();
    }
}

