package io.transwarp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

    // 用于解析ANALYZE结果的正则表达式模式
    private static final Pattern RESULT_PATTERN = Pattern.compile(
            "^([\\w.]+)(\\{([^}]+)})?\\s+\\[numFiles=(\\d+),\\s*numRows=(\\d+),\\s*totalSize=(\\d+),\\s*rawDataSize=(\\d+)]$"
    );

    // 时间格式化器
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // 目标表的DDL定义（包含分区和存储格式）
    private static final String CREATE_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS " + TARGET_TABLE_NAME + " (\n" +
                    "    id STRING COMMENT '唯一标识',\n" +
                    "    data_time STRING COMMENT '插入时间',\n" +
                    "    data_day DATE COMMENT '分区时间',\n" +
                    "    table_name STRING COMMENT '表名',\n" +
                    "    partition_name STRING COMMENT '分区名称',\n" +
                    "    is_partition INT COMMENT '是否分区表',\n" +
                    "    numFiles INT COMMENT '文件数',\n" +
                    "    numRows INT COMMENT '行数',\n" +
                    "    totalSize BIGINT COMMENT '总大小(字节)',\n" +
                    "    rawDataSize BIGINT COMMENT '原始数据大小(字节)'\n" +
                    ")\n" +
                    "COMMENT '存放inceptor每张表的统计信息'\n" +
                    "PARTITIONED BY RANGE (data_day)\n" +
                    "INTERVAL (numtoyminterval('1','month'))\n" +
                    "(\n" +
                    "  PARTITION p VALUES LESS THAN ('1970-01-01')\n" +
                    ")\n" +
                    "CLUSTERED BY (id) INTO 11 BUCKETS STORED AS ORC\n" +
                    "TBLPROPERTIES('transactional'='true')";

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
            logger.info("Creating/verifying target table: {}", TARGET_TABLE_NAME);
            try (Connection conn = writeDs.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_DDL);
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
            logger.info("Task queue initialized with capacity: {}", taskQueueCapacity);
            
            // 从系统表加载需要分析的表
            logger.info("Loading analysis tasks from database...");
            int totalTasks = loadAnalysisTasks(analyzeDs, taskQueue);
            logger.info("Total analysis tasks loaded: {}", totalTasks);

            // 初始化工作线程池（并发执行ANALYZE命令）
            logger.info("Starting {} analyze worker threads...", CONCURRENCY);
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
            for (int i = 0; i < CONCURRENCY; i++) {
                executor.submit(new AnalyzeWorker(analyzeDs, taskQueue, i + 1));
            }
            logger.info("All {} analyze worker threads started", CONCURRENCY);

            // 等待所有分析任务完成
            logger.info("Waiting for all analysis tasks to complete (max wait: 2 hours)...");
            executor.shutdown();
            boolean completed = executor.awaitTermination(2, TimeUnit.HOURS);
            if (completed) {
                logger.info("All analysis tasks completed successfully");
            } else {
                logger.warn("Analysis tasks did not complete within timeout period");
            }

            // 发送终止信号给写入线程
            logger.info("Sending termination signal to batch writer thread...");
            resultQueue.put(new String[]{POISON_PILL});
            writerService.shutdown();
            boolean writerCompleted = writerService.awaitTermination(30, TimeUnit.MINUTES);
            if (writerCompleted) {
                logger.info("Batch writer thread completed successfully");
            } else {
                logger.warn("Batch writer thread did not complete within timeout period");
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
                    
                    try (Connection conn = dataSource.getConnection();
                         Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) { // 执行ANALYZE并获取结果
                        int count = 0;
                        int parsedCount = 0;
                        while (rs.next()) {
                            String resultLine = rs.getString(1) + " " + rs.getString(2);
                            Optional<String[]> parsed = parseResultLine(resultLine); // 解析结果行
                            if (parsed.isPresent()) {
                                resultQueue.put(parsed.get());  // 将解析结果放入结果队列
                                parsedCount++;
                            }
                            count++;
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

        // 解析ANALYZE命令返回的结果行
        private Optional<String[]> parseResultLine(String line) {
            Matcher m = RESULT_PATTERN.matcher(line.trim());
            if (m.matches()) {
                return Optional.of(new String[]{
                        m.group(1),   // tablePart（表名）
                        m.group(3),   // partition（分区名）
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
    }

    // 结果批量写入线程：负责将统计信息批量写入目标表
    static class ResultBatchWriter implements Runnable {
        private final HikariDataSource dataSource;
        // 插入SQL语句（注意字段顺序与目标表结构一致）
        private static final String INSERT_SQL =
                "INSERT INTO " + TARGET_TABLE_NAME +
                        "(id, data_time, data_day, table_name, partition_name, is_partition, " +
                        "numFiles, numRows, totalSize, rawDataSize) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?)";

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
                 PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

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
                
                // 判断是否为分区表
                int isPartition = partition.isEmpty() ? 0 : 1;
                String tablePartition = tableName;  // 始终使用表名作为id的一部分
                String partitionName = partition.isEmpty() ? "" : partition;
                // 生成UUID
                String uuid = UUID.randomUUID().toString();
                // 设置PreparedStatement参数
                try {
                    ps.setString(1, uuid);   // id
                    ps.setString(2, dataTime);                       // data_time
                    ps.setString(3, dataDay);                        // data_day
                    ps.setString(4, tablePartition);                 // table_name
                    ps.setString(5, partitionName);                  // partition_name ← 新增
                    ps.setInt(6, isPartition);                       // is_partition
                    ps.setInt(7, parseIntWithDefault(row[2]));       // numFiles
                    ps.setInt(8, parseIntWithDefault(row[3]));       // numRows
                    ps.setLong(9, parseLongWithDefault(row[4]));     // totalSize
                    ps.setLong(10, parseLongWithDefault(row[5]));    // rawDataSize
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

                int count = 0;
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    // 为每个表生成ANALYZE命令并加入任务队列
                    queue.put("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS;");
                    count++;
                    totalTasks++;
                }
                hasMore = count >= PAGE_SIZE; // 判断是否还有更多数据
                page++;
                logger.debug("Loaded analysis tasks, page: {}, count: {}, total so far: {}", 
                        page, String.format("count:%d, total:%d", count, totalTasks));
            } catch (SQLException | InterruptedException e) {
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
}