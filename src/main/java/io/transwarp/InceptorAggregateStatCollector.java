package io.transwarp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.*;
import java.util.regex.*;

public class InceptorAggregateStatCollector {
    // 新增统计信息承载类
    private static class TableStats {
        int numFiles = 0;
        int numRows = 0;
        long totalSize = 0;
        long rawDataSize = 0;
    }

    // 从配置读取
    private static final int CONCURRENCY = ConfigUtil.getInt("concurrency.level");
    private static final int BATCH_SIZE = ConfigUtil.getInt("batch.size");
    private static final int RESULT_QUEUE_CAPACITY = ConfigUtil.getInt("result.queue.capacity");
    private static final int PAGE_SIZE = ConfigUtil.getInt("page.size");
    private static final String TARGET_TABLE_NAME = ConfigUtil.getString("inceptor.target.table.name");
    private static final String POISON_PILL = "--TERMINATE--";
    private static final Pattern RESULT_PATTERN = Pattern.compile(
            "^([\\w.]+)(\\{([^}]+)})?\\s+\\[numFiles=(\\d+),\\s*numRows=(\\d+),\\s*totalSize=(\\d+),\\s*rawDataSize=(\\d+)]$"
    );
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // 建表语句常量
    private static final String CREATE_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS " + TARGET_TABLE_NAME + " (\n" +
                    "    id STRING COMMENT '唯一标识',\n" +
                    "    data_time STRING COMMENT '插入时间',\n" +
                    "    data_day DATE COMMENT '分区时间',\n" +
                    "    table_partition STRING COMMENT '表/分区标识',\n" +
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

    // 结果队列用于批量写入
    private static final BlockingQueue<String[]> resultQueue = new LinkedBlockingQueue<>(RESULT_QUEUE_CAPACITY);

    public static void main(String[] args) {
        // 分析用连接池配置
        HikariConfig analyzeConfig = new HikariConfig();
        analyzeConfig.setJdbcUrl(ConfigUtil.getString("jdbc.url"));
        analyzeConfig.setDriverClassName(ConfigUtil.getString("jdbc.driver"));
        analyzeConfig.setUsername(ConfigUtil.getString("jdbc.username"));
        analyzeConfig.setPassword(ConfigUtil.getString("jdbc.password"));
        analyzeConfig.setConnectionTimeout(30000);  // 30秒连接超时
        analyzeConfig.setMaximumPoolSize(CONCURRENCY);

        // 写入用独立连接池配置
        HikariConfig writeConfig = new HikariConfig();
        writeConfig.setJdbcUrl(analyzeConfig.getJdbcUrl());
        writeConfig.setDriverClassName(analyzeConfig.getDriverClassName());
        writeConfig.setUsername(analyzeConfig.getUsername());
        writeConfig.setPassword(analyzeConfig.getPassword());
        writeConfig.setConnectionTimeout(30000);  // 30秒连接超时
        writeConfig.setMaximumPoolSize(2); // 限制写入并发

        try (HikariDataSource analyzeDs = new HikariDataSource(analyzeConfig);
             HikariDataSource writeDs = new HikariDataSource(writeConfig)) {


            // 新增：执行建表操作
            try (Connection conn = writeDs.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_TABLE_DDL);
            } catch (SQLException e) {
                System.err.println("建表失败:" + e.getMessage());
            }

            // 启动批量写入线程
            ExecutorService writerService = Executors.newSingleThreadExecutor();
            writerService.submit(new ResultBatchWriter(writeDs));

            // SQL任务队列
            BlockingQueue<String> taskQueue = new LinkedBlockingQueue<>(ConfigUtil.getInt("task.queue.capacity"));
            loadAnalysisTasks(analyzeDs, taskQueue);


            // 工作线程池
            ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
            for (int i = 0; i < CONCURRENCY; i++) {
                executor.submit(new AnalyzeWorker(analyzeDs, taskQueue));
            }

            // 等待所有分析任务完成
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.HOURS);

            // 发送结束标记给写入线程
            resultQueue.put(new String[]{POISON_PILL});
            writerService.shutdown();
            writerService.awaitTermination(30, TimeUnit.MINUTES);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class AnalyzeWorker implements Runnable {
        private final HikariDataSource dataSource;
        private final BlockingQueue<String> taskQueue;

        AnalyzeWorker(HikariDataSource ds, BlockingQueue<String> queue) {
            this.dataSource = ds;
            this.taskQueue = queue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String sql = taskQueue.take();
                    if (POISON_PILL.equals(sql)) break;

                    log("开始分析: " + sql);
                    long startTime = System.currentTimeMillis();

                    try (Connection conn = dataSource.getConnection();
                         Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {

                        Map<String, TableStats> tableStats = new HashMap<>();
                        List<String[]> partitionRecords = new ArrayList<>();

                        while (rs.next()) {
                            String resultLine = rs.getString(1) + " " + rs.getString(2);
                            Optional<String[]> parsed = parseResultLine(resultLine);
                            if (!parsed.isPresent()) continue;

                            String[] parts = parsed.get();
                            String tableName = parts[0];
                            String partition = parts[1];

                            if (!partition.isEmpty()) { // 分区记录
                                TableStats stats = tableStats.computeIfAbsent(tableName, k -> new TableStats());
                                stats.numFiles += Integer.parseInt(parts[2]);
                                stats.numRows += Integer.parseInt(parts[3]);
                                stats.totalSize += Long.parseLong(parts[4]);
                                stats.rawDataSize += Long.parseLong(parts[5]);
                                partitionRecords.add(parts); // 保留原始分区记录
                            } else { // 非分区表
                                resultQueue.put(parts);
                            }
                        }

                        // 生成聚合记录
                        tableStats.forEach((tableName, stats) -> {
                            String[] summary = {
                                    tableName,
                                    "", // 空分区
                                    String.valueOf(stats.numFiles),
                                    String.valueOf(stats.numRows),
                                    String.valueOf(stats.totalSize),
                                    String.valueOf(stats.rawDataSize)
                            };
                            try {
                                resultQueue.put(summary);
                                log("聚合完成: " + tableName +
                                        " 分区数: " + partitionRecords.stream()
                                        .filter(p -> p[0].equals(tableName))
                                        .count());
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

                        log("分析完成: " + sql +
                                " 耗时: " + (System.currentTimeMillis() - startTime) + "ms" +
                                " 分区数: " + partitionRecords.size() +
                                " 聚合表数: " + tableStats.size());
                    } catch (SQLException e) {
                        logError("分析失败: " + sql, e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private Optional<String[]> parseResultLine(String line) {
            Matcher m = RESULT_PATTERN.matcher(line.trim());
            if (m.matches()) {
                String tablePart = m.group(1);  // db.table
                String partition = m.group(3);  // 分区名或null
                return Optional.of(new String[]{
                        tablePart,
                        partition != null ? partition : "",
                        m.group(4), // numFiles
                        m.group(5), // numRows
                        m.group(6), // totalSize
                        m.group(7)  // rawDataSize
                });
            }
            return Optional.empty();
        }
    }



    // 增强日志方法
    private static void log(String message) {
        String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
        System.out.printf("[%s][INFO] %s%n", timestamp, message);
    }

    private static void logError(String message, Exception e) {
        String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
        System.err.printf("[%s][ERROR] %s - %s%n", timestamp, message, e.getMessage());
        e.printStackTrace(System.err);
    }

    static class ResultBatchWriter implements Runnable {
        private final HikariDataSource dataSource;
        // 插入正确表名和字段顺序
        private static final String INSERT_SQL =
                "INSERT INTO " + TARGET_TABLE_NAME  +
                        "(id, data_time, data_day, table_partition, numFiles, numRows, totalSize, rawDataSize) " +
                        "VALUES (?,?,?,?,?,?,?,?)";
        ResultBatchWriter(HikariDataSource ds) {
            this.dataSource = ds;
        }

        @Override
        public void run() {
            List<String[]> buffer = new ArrayList<>(BATCH_SIZE);
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

                conn.setAutoCommit(false); // 启用批量提交

                while (true) {
                    String[] record = resultQueue.poll(1, TimeUnit.SECONDS);
                    if (record == null) continue;

                    if (POISON_PILL.equals(record[0])) {
                        if (!buffer.isEmpty()) {
                            executeBatch(ps, buffer);
                        }
                        break;
                    }

                    buffer.add(record);
                    if (buffer.size() >= BATCH_SIZE) {
                        executeBatch(ps, buffer);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void executeBatch(PreparedStatement ps, List<String[]> buffer) throws SQLException {
            long startTime = System.currentTimeMillis();
            // 为整个批次生成统一时间戳
            String dataTime = LocalDateTime.now().format(TIME_FORMATTER);
            // 为整个批次生成统一日期，用于表的interval分区字段
            String dataDay = LocalDateTime.now().format(DATE_FORMATTER);
            for (String[] row : buffer) {
                // 生成UUID
                String uuid = UUID.randomUUID().toString();

                // 设置8个参数
                ps.setString(1, uuid);                        // id
                ps.setString(2, dataTime);                   // data_time
                ps.setString(3, dataDay);                   // data_day

                String tablePartition = row[1].isEmpty() ?
                        row[0] :  // 非分区表
                        row[0] + "{" + row[1] + "}"; // 分区表

                ps.setString(4, tablePartition); // table_partition
                ps.setInt(5, Integer.parseInt(row[2])); // numFiles
                ps.setInt(6, Integer.parseInt(row[3])); // numRows
                ps.setLong(7, Long.parseLong(row[4])); // totalSize
                ps.setLong(8, Long.parseLong(row[5])); // rawDataSize
                ps.addBatch();
            }
            try {
                int[] counts = ps.executeBatch();
                ps.getConnection().commit();
//                System.out.printf("批量写入完成: %d条 [%s]%n",
//                        counts.length,
//                        LocalDateTime.now().format(TIME_FORMATTER));
                log(String.format("写入成功: 批次大小=%d 耗时=%dms",
                        counts.length,
                        System.currentTimeMillis()-startTime));
            } catch (SQLException e) {
                logError("批量写入失败: " + e.getMessage(), e);
                ps.getConnection().rollback();
                throw e;
            } finally {
                buffer.clear();
            }
        }
    }


    /**
     * 从inceptor系统表system.tables_v分页加载需要分析的表
     */
    private static void loadAnalysisTasks(HikariDataSource dataSource,
                                          BlockingQueue<String> queue) {
        final String baseSQL = ConfigUtil.getString("table.query.sql");
        int page = 0;
        boolean hasMore = true;

        while (hasMore) {
            String pagedSQL = String.format("%s LIMIT %d OFFSET %d",
                    baseSQL, PAGE_SIZE, page * PAGE_SIZE);

            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(pagedSQL)) {

                int count = 0;
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    queue.put("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS;");
                    count++;
                }
                hasMore = count >= PAGE_SIZE;
                page++;

                System.out.printf("已加载 %d 个分析任务（第%d页）%n", count, page);
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException("任务加载失败", e);
            }
        }

        // 添加终止标记
        for (int i = 0; i < CONCURRENCY; i++) {
            try {
                queue.put(POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}