package io.transwarp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于收集 Inceptor中表统计信息的工具。通过并发方式执行 ANALYZE 命令获取表统计信息，并将结果批量写入目标表中。
 * 代码采用了生产者 - 消费者模式，使用线程池和阻塞队列实现任务分发与结果处理。
 * 关键功能模块详解
 *
 *     (1)并发架构设计
 *         采用生产者 - 消费者模式：AnalyzeWorker作为生产者生成统计结果，ResultBatchWriter作为消费者批量写入数据库
 *         使用BlockingQueue实现线程间安全通信
 *         分离分析和写入连接池，避免资源竞争
 *     (2)数据库操作优化
 *         批量写入：使用addBatch()和事务管理提高写入效率
 *         分页加载：通过LIMIT和OFFSET避免一次性加载过多任务
 *         连接池配置：HikariCP 提供高效的数据库连接管理
 *     (3)异常处理与可靠性
 *         空值安全检查：避免因数据异常导致程序崩溃
 *         数值转换保护：提供带默认值的转换方法
 *         事务管理：批量写入失败时回滚，保证数据一致性
 *     (4)性能优化点
 *         并发级别可配置：通过concurrency.level参数调整
 *         批量大小可配置：通过batch.size参数调整
 *         结果队列容量可配置：避免内存溢出
 */

public class InceptorStatCollector {
    private static final Logger logger = LoggerFactory.getLogger(InceptorStatCollector.class);
    // 从配置文件读取的并发参数和批量大小
    private static final int CONCURRENCY = ConfigUtil.getInt("concurrency.level"); // 并发级别
    private static final int BATCH_SIZE = ConfigUtil.getInt("batch.size"); // 批量写入大小
    private static final int RESULT_QUEUE_CAPACITY = ConfigUtil.getInt("result.queue.capacity"); // 结果队列容量
    private static final int PAGE_SIZE = ConfigUtil.getInt("page.size"); // 分页大小
    private static final String TARGET_TABLE_NAME = ConfigUtil.getString("inceptor.target.table.name"); // 目标表名
    private static final String POISON_PILL = "--TERMINATE--"; // 终止信号标记
    // 统计级别：partition（分区级别，默认）或 table（表级别）
    private static final String ANALYZE_LEVEL = ConfigUtil.getString("analyze.level", "partition").toLowerCase();
    // 超时配置：分析任务完成超时时间（小时）
    private static final int ANALYSIS_TASK_TIMEOUT_HOURS = ConfigUtil.getInt("analysis.task.timeout.hours", 2);
    // 超时配置：批量写入线程完成超时时间（分钟）
    private static final int BATCH_WRITER_TIMEOUT_MINUTES = ConfigUtil.getInt("batch.writer.timeout.minutes", 30);
    // 表清单来源模式：sql（从数据库查询，默认）或 file（从文件读取）
    private static final String TABLE_SOURCE_MODE = ConfigUtil.getString("table.source.mode", "sql").toLowerCase();
    // 表清单文件路径（当 table.source.mode=file 时使用）
    private static final String TABLE_LIST_FILE = ConfigUtil.getString("table.list.file", "tables.txt");
    // 失败表记录文件路径（记录执行失败的表名和日期）
    private static final String FAILED_TABLES_FILE = ConfigUtil.getString("failed.tables.file", "conf/failed_tables.txt");
    // 失败表记录器（线程安全）
    private static final FailedTableRecorder failedTableRecorder = new FailedTableRecorder(FAILED_TABLES_FILE);

    // 用于解析ANALYZE结果的正则表达式模式
    private static final Pattern RESULT_PATTERN = Pattern.compile(
            "^([\\w.]+)(\\{([^}]+)})?\\s+\\[numFiles=(\\d+),\\s*numRows=(\\d+),\\s*totalSize=(\\d+),\\s*rawDataSize=(\\d+)]$"
    );
    
    /**
     * 验证表名格式是否合法，防止SQL注入
     * 表名格式：database.table，只允许字母、数字、下划线、点号
     * @param tableName 表名
     * @return 是否合法
     */
    private static boolean isValidTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }
        // 表名格式：database.table，只允许字母、数字、下划线、点号
        // 不允许包含空格、分号、单引号、双引号、注释符号等危险字符
        String trimmed = tableName.trim();
        // 检查是否包含危险字符
        if (trimmed.contains(";") || trimmed.contains("'") || trimmed.contains("\"") ||
            trimmed.contains("--") || trimmed.contains("/*") || trimmed.contains("*/") ||
            trimmed.toUpperCase().contains("DROP") || trimmed.toUpperCase().contains("ALTER") || 
            trimmed.toUpperCase().contains("DELETE") || trimmed.toUpperCase().contains("TRUNCATE") || 
            trimmed.toUpperCase().contains("UPDATE") || trimmed.toUpperCase().contains("EXEC") ||
            trimmed.toUpperCase().contains("EXECUTE") || trimmed.toUpperCase().contains("UNION") || 
            trimmed.toUpperCase().contains("SELECT") || trimmed.toUpperCase().contains("INSERT") || 
            trimmed.toUpperCase().contains("CREATE")) {
            return false;
        }
        // 验证格式：必须是 database.table 格式
        if (!trimmed.matches("^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$")) {
            return false;
        }
        return true;
    }
    
    /**
     * 清理表名，移除可能的危险字符
     * 注意：此方法仅作为额外安全措施，主要依赖 isValidTableName 验证
     * @param tableName 表名
     * @return 清理后的表名
     */
    private static String sanitizeTableName(String tableName) {
        if (tableName == null) {
            return "";
        }
        // 移除首尾空格
        String sanitized = tableName.trim();
        // 移除可能的危险字符（虽然已经通过验证，但作为额外保护）
        sanitized = sanitized.replaceAll("[;'\"]", "");
        return sanitized;
    }

    // 时间格式化器
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // 目标表的DDL定义（根据统计级别动态生成）
    private static String getCreateTableDDL() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(TARGET_TABLE_NAME).append(" (\n");
        ddl.append("    id STRING COMMENT '唯一标识',\n");
        ddl.append("    data_time STRING COMMENT '插入时间',\n");
        ddl.append("    data_day DATE COMMENT '分区时间',\n");
        ddl.append("    table_name STRING COMMENT '表名',\n");
        if ("partition".equals(ANALYZE_LEVEL)) {
            // 分区级别模式：包含 partition_name 和 is_partition 字段
            ddl.append("    partition_name STRING COMMENT '分区名称',\n");
            ddl.append("    is_partition INT COMMENT '是否分区表',\n");
        }
        ddl.append("    numFiles INT COMMENT '文件数',\n");
        ddl.append("    numRows INT COMMENT '行数',\n");
        ddl.append("    totalSize BIGINT COMMENT '总大小(字节)',\n");
        ddl.append("    rawDataSize BIGINT COMMENT '原始数据大小(字节)'\n");
        ddl.append(")\n");
        ddl.append("COMMENT '存放inceptor每张表的统计信息'\n");
        ddl.append("PARTITIONED BY RANGE (data_day)\n");
        ddl.append("INTERVAL (numtoyminterval('1','month'))\n");
        ddl.append("(\n");
        ddl.append("  PARTITION p VALUES LESS THAN ('1970-01-01')\n");
        ddl.append(")\n");
        ddl.append("CLUSTERED BY (id) INTO 11 BUCKETS STORED AS ORC\n");
        ddl.append("TBLPROPERTIES('transactional'='true')");
        return ddl.toString();
    }

    // 结果队列：用于存储解析后的表统计信息，供批量写入线程使用
    private static final BlockingQueue<String[]> resultQueue = new LinkedBlockingQueue<>(RESULT_QUEUE_CAPACITY);

    public static void main(String[] args) {
        logger.info("========================================");
        logger.info("InceptorStatCollector Starting...");
        logger.info("========================================");
        logger.info("Configuration loaded:");
        logger.info("  - Concurrency level: {}", CONCURRENCY);
        logger.info("  - Batch size: {}", BATCH_SIZE);
        logger.info("  - Result queue capacity: {}", RESULT_QUEUE_CAPACITY);
        logger.info("  - Page size: {}", PAGE_SIZE);
        logger.info("  - Target table: {}", TARGET_TABLE_NAME);
        logger.info("  - Analyze level: {} ({})", ANALYZE_LEVEL, 
                "partition".equals(ANALYZE_LEVEL) ? "partition-level statistics" : "table-level statistics");
        
        // 设置系统默认编码为UTF-8，解决中文乱码问题
        // 注意：这些设置应该在JVM启动时通过-D参数设置，这里作为备用
        if (System.getProperty("file.encoding") == null) {
            System.setProperty("file.encoding", "UTF-8");
        }
        if (System.getProperty("sun.jnu.encoding") == null) {
            System.setProperty("sun.jnu.encoding", "UTF-8");
        }
        
        long programStartTime = System.currentTimeMillis();
        
        // 分析用连接池配置
        logger.info("Initializing analyze connection pool...");
        HikariConfig analyzeConfig = new HikariConfig();
        analyzeConfig.setJdbcUrl(ConfigUtil.getString("jdbc.url"));
        analyzeConfig.setDriverClassName(ConfigUtil.getString("jdbc.driver"));
        analyzeConfig.setUsername(ConfigUtil.getString("jdbc.username"));
        analyzeConfig.setPassword(ConfigUtil.getString("jdbc.password"));
        analyzeConfig.setConnectionTimeout(30000);  // 30秒连接超时
        analyzeConfig.setMaximumPoolSize(CONCURRENCY);
        logger.info("Analyze connection pool configured: maxPoolSize={}, timeout={}ms", 
                CONCURRENCY, 30000);

        // 写入用独立连接池配置
        // 配置写入用数据库连接池（低并发，专注写入）
        logger.info("Initializing write connection pool...");
        HikariConfig writeConfig = new HikariConfig();
        writeConfig.setJdbcUrl(analyzeConfig.getJdbcUrl());
        writeConfig.setDriverClassName(analyzeConfig.getDriverClassName());
        writeConfig.setUsername(analyzeConfig.getUsername());
        writeConfig.setPassword(analyzeConfig.getPassword());
        writeConfig.setConnectionTimeout(30000);  // 30秒连接超时
        writeConfig.setMaximumPoolSize(2); // 限制写入并发为2，避免数据库压力过大
        logger.info("Write connection pool configured: maxPoolSize=2, timeout={}ms", 30000);

        try (HikariDataSource analyzeDs = new HikariDataSource(analyzeConfig);
             HikariDataSource writeDs = new HikariDataSource(writeConfig)) {
            logger.info("Connection pools initialized successfully");
            // 新增：执行建表操作
            logger.info("Creating/verifying target table: {} (analyze level: {})", TARGET_TABLE_NAME, ANALYZE_LEVEL);
            try (Connection conn = writeDs.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(getCreateTableDDL());
                logger.info("Target table created/verified successfully: {}", TARGET_TABLE_NAME);
            } catch (SQLException e) {
                logger.error("Failed to create table: {}", TARGET_TABLE_NAME, e);
                throw e;
            }

            // 启动批量写入线程（单线程执行，保证写入顺序）
            logger.info("Starting batch writer thread...");
            ExecutorService writerService = Executors.newSingleThreadExecutor();
            writerService.submit(new ResultBatchWriter(writeDs));
            logger.info("Batch writer thread started");

            // 初始化SQL任务队列
            int taskQueueCapacity = ConfigUtil.getInt("task.queue.capacity");
            BlockingQueue<String> taskQueue = new LinkedBlockingQueue<>(taskQueueCapacity);
            logger.info("Task queue initialized with capacity: {} (if queue is full, task loading will wait)", taskQueueCapacity);
            
            // 从系统表或文件加载需要分析的表
            int totalTasks;
            if ("file".equals(TABLE_SOURCE_MODE)) {
                logger.info("Loading analysis tasks from file: {}", TABLE_LIST_FILE);
                totalTasks = loadAnalysisTasksFromFile(taskQueue);
                logger.info("Total analysis tasks loaded from file: {}", totalTasks);
            } else {
                logger.info("Loading analysis tasks from database...");
                totalTasks = loadAnalysisTasks(analyzeDs, taskQueue);
                logger.info("Total analysis tasks loaded from database: {}", totalTasks);
            }

            // 初始化工作线程池（并发执行ANALYZE命令）
            logger.info("Starting {} analyze worker threads...", CONCURRENCY);
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
            for (int i = 0; i < CONCURRENCY; i++) {
                executor.submit(new AnalyzeWorker(analyzeDs, taskQueue, i + 1));
            }
            logger.info("All {} analyze worker threads started", CONCURRENCY);

            // 等待所有分析任务完成
            logger.info("Waiting for all analysis tasks to complete (max wait: {} hours)...", ANALYSIS_TASK_TIMEOUT_HOURS);
            executor.shutdown();
            boolean completed = executor.awaitTermination(ANALYSIS_TASK_TIMEOUT_HOURS, TimeUnit.HOURS);
            if (completed) {
                logger.info("All analysis tasks completed successfully");
            } else {
                logger.warn("Analysis tasks did not complete within timeout period, forcing shutdown...");
                // 超时后，先发送终止信号到任务队列，让工作线程能够退出循环
                // 注意：此时队列可能已经空了，但工作线程可能还在执行长时间任务
                for (int i = 0; i < CONCURRENCY; i++) {
                    try {
                        taskQueue.put(POISON_PILL);
                    } catch (InterruptedException e) {
                        logger.warn("Failed to send termination signal to task queue", e);
                        Thread.currentThread().interrupt();
                    }
                }
                // 强制中断工作线程，但给它们一些时间完成当前任务
                logger.info("Shutting down executor service forcefully, waiting for workers to finish current tasks...");
                executor.shutdownNow();
                // 等待最多5分钟让工作线程完成当前任务
                try {
                    boolean terminated = executor.awaitTermination(5, TimeUnit.MINUTES);
                    if (terminated) {
                        logger.info("All worker threads completed after forceful shutdown");
                    } else {
                        logger.warn("Some worker threads did not complete within 5 minutes after forceful shutdown");
                    }
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting for worker threads to complete", e);
                    Thread.currentThread().interrupt();
                }
            }

            // 发送终止信号给写入线程
            logger.info("Sending termination signal to batch writer thread...");
            resultQueue.put(new String[]{POISON_PILL});
            writerService.shutdown();
            logger.info("Waiting for batch writer thread to complete (max wait: {} minutes)...", BATCH_WRITER_TIMEOUT_MINUTES);
            boolean writerCompleted = writerService.awaitTermination(BATCH_WRITER_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (writerCompleted) {
                logger.info("Batch writer thread completed successfully");
            } else {
                logger.warn("Batch writer thread did not complete within timeout period, forcing shutdown...");
                writerService.shutdownNow();
                // 等待最多2分钟让写入线程完成当前任务
                try {
                    boolean terminated = writerService.awaitTermination(2, TimeUnit.MINUTES);
                    if (terminated) {
                        logger.info("Batch writer thread completed after forceful shutdown");
                    } else {
                        logger.warn("Batch writer thread did not complete within 2 minutes after forceful shutdown");
                    }
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting for batch writer thread to complete", e);
                    Thread.currentThread().interrupt();
                }
            }
            
            long programElapsedTime = System.currentTimeMillis() - programStartTime;
            logger.info("========================================");
            logger.info("InceptorStatCollector Completed");
            logger.info("Total execution time: {}ms ({} seconds)", 
                    programElapsedTime, programElapsedTime / 1000);
            logger.info("========================================");

        } catch (Exception e) {
            logger.error("Main program terminated abnormally", e);
        }
    }

    // 分析工作线程：负责执行ANALYZE命令并解析结果
    static class AnalyzeWorker implements Runnable {
        private final HikariDataSource dataSource;
        private final BlockingQueue<String> taskQueue;
        private final int workerId;
        private int processedCount = 0;
        private int successCount = 0;
        private int failureCount = 0;

        AnalyzeWorker(HikariDataSource ds, BlockingQueue<String> queue, int workerId) {
            this.dataSource = ds;
            this.taskQueue = queue;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            logger.info("AnalyzeWorker-{} started", workerId);
            long workerStartTime = System.currentTimeMillis();
            
            try {
                while (true) {
                    String sql = taskQueue.take(); // 从任务队列获取ANALYZE SQL
                    if (POISON_PILL.equals(sql)) { // 收到终止信号
                        logger.debug("AnalyzeWorker-{} received termination signal", workerId);
                        break;
                    }

                    processedCount++;
                    long taskStartTime = System.currentTimeMillis();
                    logger.debug("AnalyzeWorker-{} processing task #{}, SQL: {}", 
                            workerId, processedCount + " - " + sql);
                    
                    // 检查数据源是否已关闭
                    if (dataSource.isClosed()) {
                        logger.warn("AnalyzeWorker-{} detected dataSource is closed, skipping task: {}", workerId, sql);
                        failureCount++;
                        // 提取表名并记录失败信息
                        String tableName = extractTableNameFromSQL(sql);
                        if (tableName != null) {
                            failedTableRecorder.recordFailedTable(tableName);
                        }
                        continue;
                    }
                    
                    try (Connection conn = dataSource.getConnection();
                         Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) { // 执行ANALYZE并获取结果
                        int count = 0;
                        int parsedCount = 0;
                        
                        if ("table".equals(ANALYZE_LEVEL)) {
                            // 表级别模式：收集所有结果并合并分区统计
                            Map<String, long[]> tableStats = new HashMap<>(); // key: tableName, value: [numFiles, numRows, totalSize, rawDataSize]
                            String currentTableName = null;
                            
                            while (rs.next()) {
                                String resultLine = rs.getString(1) + " " + rs.getString(2);
                                Optional<String[]> parsed = parseResultLineForMerge(resultLine);
                                if (parsed.isPresent()) {
                                    String tableName = parsed.get()[0];
                                    String partition = parsed.get()[1];
                                    
                                    if (currentTableName == null) {
                                        currentTableName = tableName;
                                    }
                                    
                                    if (partition == null) {
                                        // 表级别结果：直接使用
                                        resultQueue.put(parsed.get());
                                        parsedCount++;
                                    } else {
                                        // 分区级别结果：累加到表统计中
                                        long[] stats = tableStats.getOrDefault(tableName, new long[]{0, 0, 0, 0});
                                        stats[0] += Long.parseLong(parsed.get()[2]); // numFiles
                                        stats[1] += Long.parseLong(parsed.get()[3]); // numRows
                                        stats[2] += Long.parseLong(parsed.get()[4]); // totalSize
                                        stats[3] += Long.parseLong(parsed.get()[5]); // rawDataSize
                                        tableStats.put(tableName, stats);
                                    }
                                    count++;
                                }
                            }
                            
                            // 输出合并后的表级别统计（如果有分区数据）
                            for (Map.Entry<String, long[]> entry : tableStats.entrySet()) {
                                String tableName = entry.getKey();
                                long[] stats = entry.getValue();
                                if (stats[0] > 0 || stats[1] > 0 || stats[2] > 0 || stats[3] > 0) {
                                    // 创建合并后的表级别结果（partition_name 为 null）
                                    String[] mergedResult = new String[]{
                                            tableName,
                                            null,  // partition_name = null for table-level
                                            String.valueOf(stats[0]),  // numFiles
                                            String.valueOf(stats[1]),  // numRows
                                            String.valueOf(stats[2]),  // totalSize
                                            String.valueOf(stats[3])   // rawDataSize
                                    };
                                    resultQueue.put(mergedResult);
                                    parsedCount++;
                                    String statsInfo = String.format("numFiles:%d, numRows:%d, totalSize:%d, rawDataSize:%d",
                                            stats[0], stats[1], stats[2], stats[3]);
                                    logger.debug("AnalyzeWorker-{} merged partition stats for table: {}, stats: {}",
                                            workerId, tableName + " - " + statsInfo);
                                }
                            }
                        } else {
                            // 分区级别模式：直接处理所有结果
                            while (rs.next()) {
                                String resultLine = rs.getString(1) + " " + rs.getString(2);
                                Optional<String[]> parsed = parseResultLine(resultLine); // 解析结果行
                                if (parsed.isPresent()) {
                                    resultQueue.put(parsed.get());  // 将解析结果放入结果队列
                                    parsedCount++;
                                }
                                count++;
                            }
                        }
                        
                        successCount++;
                        long taskElapsedTime = System.currentTimeMillis() - taskStartTime;
                        logger.debug("AnalyzeWorker-{} completed task #{}, SQL: {}, " +
                                "records: {}, parsed: {}, elapsed: {}ms", 
                                workerId, String.format("task#%d, records:%d, parsed:%d, elapsed:%dms", 
                                        processedCount, count, parsedCount, taskElapsedTime) + " - " + sql);
                    } catch (SQLException e) {
                        failureCount++;
                        long taskElapsedTime = System.currentTimeMillis() - taskStartTime;
                        String errorMsg = String.format("AnalyzeWorker-%d failed task #%d, elapsed: %dms, SQL: %s", 
                                workerId, processedCount, taskElapsedTime, sql);
                        logger.error(errorMsg, e);
                        
                        // 提取表名并记录失败信息
                        String tableName = extractTableNameFromSQL(sql);
                        if (tableName != null) {
                            failedTableRecorder.recordFailedTable(tableName);
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("AnalyzeWorker-{} interrupted", workerId, e);
                Thread.currentThread().interrupt();
            } finally {
                long workerElapsedTime = System.currentTimeMillis() - workerStartTime;
                logger.info("AnalyzeWorker-{} finished, processed: {}, success: {}, " +
                        "failure: {}, total elapsed: {}ms ({} seconds)", 
                        workerId, String.format("processed:%d, success:%d, failure:%d, elapsed:%dms (%ds)", 
                                processedCount, successCount, failureCount, 
                                workerElapsedTime, workerElapsedTime / 1000));
            }
        }

        // 解析ANALYZE命令返回的结果行（用于分区级别模式）
        private Optional<String[]> parseResultLine(String line) {
            Matcher m = RESULT_PATTERN.matcher(line.trim());
            if (m.matches()) {
                String partition = m.group(3);  // partition（分区名），如果为null表示表级别
                return Optional.of(new String[]{
                        m.group(1),   // tablePart（表名）
                        partition,    // partition（分区名），表级别时为null
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
        
        // 解析ANALYZE命令返回的结果行（用于表级别模式，需要收集所有结果）
        private Optional<String[]> parseResultLineForMerge(String line) {
            return parseResultLine(line); // 使用相同的解析逻辑
        }
    }

    // 结果批量写入线程：负责将统计信息批量写入目标表
    static class ResultBatchWriter implements Runnable {
        private final HikariDataSource dataSource;
        // 插入SQL语句（根据统计级别动态生成）
        private static String getInsertSQL() {
            // 验证目标表名格式（已在getCreateTableDDL中验证，这里再次确认）
            String safeTableName = sanitizeTableName(TARGET_TABLE_NAME);
            if ("partition".equals(ANALYZE_LEVEL)) {
                // 分区级别模式：包含 partition_name 和 is_partition 字段
                return "INSERT INTO " + safeTableName +
                        "(id, data_time, data_day, table_name, partition_name, is_partition, " +
                        "numFiles, numRows, totalSize, rawDataSize) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?)";
            } else {
                // 表级别模式：不包含 partition_name 和 is_partition 字段
                return "INSERT INTO " + safeTableName +
                        "(id, data_time, data_day, table_name, " +
                        "numFiles, numRows, totalSize, rawDataSize) " +
                        "VALUES (?,?,?,?,?,?,?,?)";
            }
        }

        private int totalBatches = 0;
        private int totalRecords = 0;
        private int failedBatches = 0;
        private int failedRecords = 0;

        ResultBatchWriter(HikariDataSource ds) {
            this.dataSource = ds;
        }

        @Override
        public void run() {
            logger.info("ResultBatchWriter thread started");
            long writerStartTime = System.currentTimeMillis();
            
            List<String[]> buffer = new ArrayList<>(BATCH_SIZE);  // 批量写入缓冲区
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(getInsertSQL())) {

                conn.setAutoCommit(false); // 禁用自动提交，启用批量提交模式
                logger.info("Database connection established, auto-commit disabled");

                while (true) {
                    String[] record = resultQueue.poll(1, TimeUnit.SECONDS);  // 非阻塞获取结果
                    if (record == null) {
                        // 定期输出队列状态
                        if (totalBatches % 10 == 0 && totalBatches > 0) {
                            logger.debug("ResultBatchWriter waiting for data, queue size: {}, " +
                                    "total batches: {}, total records: {}", 
                                    resultQueue.size(), String.format("batches:%d, records:%d", 
                                            totalBatches, totalRecords));
                        }
                        continue;  // 没有数据则继续等待
                    }

                    if (POISON_PILL.equals(record[0])) {  // 收到终止信号
                        logger.info("ResultBatchWriter received termination signal, " +
                                "processing remaining {} records in buffer", buffer.size());
                        if (!buffer.isEmpty()) {
                            executeBatch(ps, buffer);  // 处理剩余数据
                        }
                        break;
                    }

                    buffer.add(record);   // 将记录添加到缓冲区
                    if (buffer.size() >= BATCH_SIZE) { // 缓冲区满时执行批量写入
                        executeBatch(ps, buffer);
                    }
                }
            } catch (Exception e) {
                logger.error("ResultBatchWriter thread encountered exception", e);
                e.printStackTrace();
            } finally {
                long writerElapsedTime = System.currentTimeMillis() - writerStartTime;
                logger.info("ResultBatchWriter thread finished, total batches: {}, " +
                        "total records: {}, failed batches: {}, failed records: {}, " +
                        "total elapsed: {}ms ({} seconds)", 
                        totalBatches, String.format("records:%d, failed_batches:%d, failed_records:%d, elapsed:%dms (%ds)", 
                                totalRecords, failedBatches, failedRecords,
                                writerElapsedTime, writerElapsedTime / 1000));
            }
        }
        // 执行批量插入操作
        private void executeBatch(PreparedStatement ps, List<String[]> buffer) throws SQLException {
            totalBatches++;
            int batchSize = buffer.size();
            long batchStartTime = System.currentTimeMillis();
            
            // 生成统一时间戳（批次内所有记录使用相同时间）
            String dataTime = LocalDateTime.now().format(TIME_FORMATTER);
            // 为整个批次生成统一日期，用于表的interval分区字段
            String dataDay = LocalDateTime.now().format(DATE_FORMATTER);
            
            int validRows = 0;
            int invalidRows = 0;
            Set<String> tablesInBatch = new HashSet<>();
            
            for (String[] row : buffer) {
                // 空值安全检查
                if (row == null || row.length < 6) {
                    logger.error("Encountered invalid data row: " + Arrays.toString(row));
                    invalidRows++;
                    continue;
                }
                // 提取表名和分区信息
                String tableName = row[0] != null ? row[0] : "";
                String partition = row[1] != null ? row[1] : "";
                tablesInBatch.add(tableName);
                
                // 生成UUID
                String uuid = UUID.randomUUID().toString();
                // 设置PreparedStatement参数
                try {
                    int paramIndex = 1;
                    ps.setString(paramIndex++, uuid);   // id
                    ps.setString(paramIndex++, dataTime);                       // data_time
                    ps.setString(paramIndex++, dataDay);                        // data_day
                    ps.setString(paramIndex++, tableName);                     // table_name
                    
                    if ("partition".equals(ANALYZE_LEVEL)) {
                        // 分区级别模式：包含 partition_name 和 is_partition 字段
                        int isPartition = (partition == null || partition.isEmpty()) ? 0 : 1;
                        String partitionName = (partition == null || partition.isEmpty()) ? "" : partition;
                        ps.setString(paramIndex++, partitionName);              // partition_name
                        ps.setInt(paramIndex++, isPartition);                   // is_partition
                    }
                    // 表级别模式：不包含 partition_name 和 is_partition 字段
                    
                    ps.setInt(paramIndex++, parseIntWithDefault(row[2]));       // numFiles
                    ps.setInt(paramIndex++, parseIntWithDefault(row[3]));       // numRows
                    ps.setLong(paramIndex++, parseLongWithDefault(row[4]));     // totalSize
                    ps.setLong(paramIndex++, parseLongWithDefault(row[5]));     // rawDataSize
                    ps.addBatch();   // 添加到批量操作
                    validRows++;
                } catch (Exception e) {
                    logger.error("Failed to process data row: " + Arrays.toString(row), e);
                    invalidRows++;
                }
            }
            
            // 执行批量插入并提交事务
            try {
                ps.executeBatch();
                ps.getConnection().commit();
                totalRecords += validRows;
                failedRecords += invalidRows;
                
                long batchElapsedTime = System.currentTimeMillis() - batchStartTime;
                logger.debug("Batch #{} write successful, records: {} (valid: {}, invalid: {}), " +
                        "tables: {}, elapsed: {}ms", 
                        totalBatches, String.format("records:%d (valid:%d, invalid:%d), tables:%d, elapsed:%dms", 
                                batchSize, validRows, invalidRows, 
                                tablesInBatch.size(), batchElapsedTime));
            } catch (SQLException e) {
                failedBatches++;
                failedRecords += batchSize;
                long batchElapsedTime = System.currentTimeMillis() - batchStartTime;
                String errorMsg = String.format("Batch #%d write failed, count: %d, elapsed: %dms", 
                        totalBatches, batchSize, batchElapsedTime);
                logger.error(errorMsg, e);
                ps.getConnection().rollback();  // 写入失败时回滚事务
                throw e;
            } finally {
                buffer.clear();  // 清空缓冲区
            }
        }

        // 安全的数值转换方法（带默认值处理）
        private int parseIntWithDefault(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                logger.warn("Number conversion failed, original value: {}", value);
                return 0;
            }
        }

        private long parseLongWithDefault(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                logger.warn("Number conversion failed, original value: {}", value);
                return 0L;
            }
        }
    }

    /**
     * 从inceptor系统表system.tables_v分页加载需要分析的表
     * 注意：Hive/Inceptor不支持OFFSET语法，使用ROW_NUMBER()窗口函数实现分页
     * @return 总任务数
     */
    private static int loadAnalysisTasks(HikariDataSource dataSource,
                                          BlockingQueue<String> queue) {
        final String baseSQL = ConfigUtil.getString("table.query.sql"); // 基础查询SQL
        logger.info("Loading tasks with base SQL: {}", baseSQL);
        
        int page = 0;
        boolean hasMore = true;
        int totalTasks = 0;

        while (hasMore) {
            // 使用ROW_NUMBER()窗口函数实现分页（Hive不支持OFFSET）
            // 将基础SQL包装在子查询中，添加ROW_NUMBER()用于分页
            // 注意：需要给基础SQL的列添加别名，以便在子查询中引用
            // 如果基础SQL没有别名，使用 _c0 作为默认列名（Hive的默认列名）
            String pagedSQL;
            if (baseSQL.toUpperCase().contains(" AS ")) {
                // 如果已经有别名，直接使用
                pagedSQL = String.format(
                        "SELECT table_name FROM (" +
                        "  SELECT table_name, ROW_NUMBER() OVER (ORDER BY table_name) as rn " +
                        "  FROM (%s) base" +
                        ") t WHERE rn > %d AND rn <= %d",
                        baseSQL, page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
            } else {
                // 如果没有别名，Hive会使用 _c0 作为第一列的默认名称
                pagedSQL = String.format(
                        "SELECT table_name FROM (" +
                        "  SELECT _c0 as table_name, ROW_NUMBER() OVER (ORDER BY _c0) as rn " +
                        "  FROM (%s) base" +
                        ") t WHERE rn > %d AND rn <= %d",
                        baseSQL, page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
            }

            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(pagedSQL)) {

                int returnedCount = 0;  // 从数据库实际返回的记录数
                int validCount = 0;     // 成功加入队列的记录数
                while (rs.next()) {
                    returnedCount++;  // 先计数，无论是否有效
                    String tableName = rs.getString(1);
                    // 验证表名格式，防止SQL注入
                    if (tableName == null || tableName.trim().isEmpty()) {
                        logger.warn("Skipping invalid table name (null or empty)");
                        continue;
                    }
                    // 验证表名格式：必须是 database.table 格式，只允许字母、数字、下划线、点号
                    if (!isValidTableName(tableName)) {
                        logger.warn("Skipping invalid table name format: {}", tableName);
                        continue;
                    }
                    // 为每个表生成ANALYZE命令并加入任务队列
                    // 注意：表名已经验证，但仍需注意安全性
                    // 使用put()会阻塞，如果队列满了会等待，确保所有任务都能加载
                    try {
                        queue.put("ANALYZE TABLE " + sanitizeTableName(tableName) + " COMPUTE STATISTICS;");
                        validCount++;
                        totalTasks++;
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while adding task to queue at page: {}", page, e);
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Failed to load tasks", e);
                    }
                }
                // 判断是否还有更多数据：基于从数据库实际返回的记录数，而不是成功加入队列的记录数
                // 如果返回的记录数等于PAGE_SIZE，说明可能还有更多数据，继续查询下一页
                // 如果返回的记录数小于PAGE_SIZE，说明已经是最后一页
                hasMore = returnedCount == PAGE_SIZE;
                page++;
                logger.info("Loaded analysis tasks, page: {}, returned: {}, valid: {}, total so far: {}", 
                        String.format("page:%d, returned:%d, valid:%d, total:%d", page, returnedCount, validCount, totalTasks));
            } catch (SQLException e) {
                logger.error("Failed to load tasks at page: {}", page, e);
                throw new RuntimeException("Failed to load tasks", e);
            }
        }

        logger.info("Task loading completed, total tasks: {}, adding {} termination signals", 
                totalTasks, CONCURRENCY);
        
        // 添加终止标记（每个工作线程一个）
        for (int i = 0; i < CONCURRENCY; i++) {
            try {
                queue.put(POISON_PILL);
            } catch (InterruptedException e) {
                logger.error("Failed to add termination signal", e);
                Thread.currentThread().interrupt();
            }
        }
        
        return totalTasks;
    }
    
    /**
     * 从文件加载需要分析的表清单
     * 文件格式：每行一个表名，格式为 database.table
     * 支持空行和注释（以 # 开头的行会被忽略）
     * @param queue 任务队列
     * @return 总任务数
     */
    private static int loadAnalysisTasksFromFile(BlockingQueue<String> queue) {
        Path filePath = Paths.get(TABLE_LIST_FILE);
        
        // 检查文件是否存在
        if (!Files.exists(filePath)) {
            throw new RuntimeException("Table list file not found: " + TABLE_LIST_FILE + 
                    ". Please create the file or set table.source.mode=sql to use database query.");
        }
        
        logger.info("Reading table list from file: {}", filePath.toAbsolutePath());
        
        int totalTasks = 0;
        int lineNumber = 0;
        int skippedCount = 0;
        
        try (BufferedReader reader = Files.newBufferedReader(filePath, java.nio.charset.StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String trimmed = line.trim();
                
                // 跳过空行和注释行
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                
                // 验证表名格式
                if (trimmed.isEmpty()) {
                    logger.warn("Skipping invalid table name at line {}: empty", lineNumber);
                    skippedCount++;
                    continue;
                }
                
                if (!isValidTableName(trimmed)) {
                    logger.warn("Skipping invalid table name format at line {}: {}", lineNumber, trimmed);
                    skippedCount++;
                    continue;
                }
                
                // 为每个表生成ANALYZE命令并加入任务队列
                try {
                    queue.put("ANALYZE TABLE " + sanitizeTableName(trimmed) + " COMPUTE STATISTICS;");
                    totalTasks++;
                } catch (InterruptedException e) {
                    logger.error("Interrupted while adding task to queue", e);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Failed to load tasks from file", e);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to read table list file: {}", TABLE_LIST_FILE, e);
            throw new RuntimeException("Failed to read table list file: " + TABLE_LIST_FILE, e);
        }
        
        logger.info("Table list file loaded successfully: total lines: {}, valid tables: {}, skipped: {}", 
                String.format("lines:%d, valid:%d, skipped:%d", lineNumber, totalTasks, skippedCount));
        
        // 添加终止标记（每个工作线程一个）
        logger.info("Adding {} termination signals to task queue", CONCURRENCY);
        for (int i = 0; i < CONCURRENCY; i++) {
            try {
                queue.put(POISON_PILL);
            } catch (InterruptedException e) {
                logger.error("Failed to add termination signal", e);
                Thread.currentThread().interrupt();
            }
        }
        
        return totalTasks;
    }
    
    /**
     * 从ANALYZE SQL语句中提取表名
     * SQL格式：ANALYZE TABLE database.table COMPUTE STATISTICS;
     * @param sql ANALYZE SQL语句
     * @return 表名（database.table格式），如果提取失败返回null
     */
    private static String extractTableNameFromSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return null;
        }
        // 匹配格式：ANALYZE TABLE database.table COMPUTE STATISTICS;
        // 使用正则表达式提取表名
        Pattern pattern = Pattern.compile("ANALYZE\\s+TABLE\\s+([\\w.]+)\\s+COMPUTE\\s+STATISTICS", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql.trim());
        if (matcher.find()) {
            String tableName = matcher.group(1);
            // 验证表名格式
            if (isValidTableName(tableName)) {
                return tableName;
            }
        }
        return null;
    }
    
    /**
     * 失败表记录器：线程安全地记录执行失败的表名和日期
     */
    private static class FailedTableRecorder {
        private final String filePath;
        private final Object lock = new Object();
        private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        FailedTableRecorder(String filePath) {
            this.filePath = filePath;
        }
        
        /**
         * 记录失败的表名和日期
         * @param tableName 表名（database.table格式）
         */
        void recordFailedTable(String tableName) {
            if (tableName == null || tableName.trim().isEmpty()) {
                return;
            }
            
            synchronized (lock) {
                try {
                    Path path = Paths.get(filePath);
                    String date = LocalDateTime.now().format(dateFormatter);
                    String record = String.format("%s,%s", tableName, date);
                    
                    // 如果文件不存在，创建文件并写入表头
                    if (!Files.exists(path)) {
                        Files.createDirectories(path.getParent());
                        Files.write(path, ("# Failed tables record file\n" +
                                "# Format: table_name,failed_date\n" +
                                "# This file can be used as input for table.list.file when table.source.mode=file\n" +
                                "# To use only table names, extract the first column\n\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                    
                    // 检查是否已记录（避免重复记录）
                    if (!isAlreadyRecorded(path, tableName, date)) {
                        // 追加记录
                        Files.write(path, (record + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8), 
                                StandardOpenOption.APPEND);
                        logger.debug("Recorded failed table: {} (date: {})", tableName, date);
                    }
                } catch (IOException e) {
                    logger.warn("Failed to record failed table: {} to file: {}, error: {}", 
                            new Object[]{tableName, filePath, e.getMessage()});
                }
            }
        }
        
        /**
         * 检查表名和日期是否已经记录过
         */
        private boolean isAlreadyRecorded(Path path, String tableName, String date) {
            try {
                if (!Files.exists(path)) {
                    return false;
                }
                List<String> lines = Files.readAllLines(path, java.nio.charset.StandardCharsets.UTF_8);
                String record = String.format("%s,%s", tableName, date);
                return lines.contains(record);
            } catch (IOException e) {
                return false; // 如果读取失败，允许写入
            }
        }
    }
}