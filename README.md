# InceptorStatCollector

用于收集 Inceptor（类 Hive）数据库中表统计信息的工具。通过并发方式执行 ANALYZE 命令获取表统计信息（包括记录数、文件数、数据大小等），并将结果批量写入目标表中。

## 功能特性

- ✅ **自动发现表**：从 `system.tables_v` 系统表自动查询需要统计的表，或从文件读取指定表清单
- ✅ **多种表清单来源**：支持从数据库查询（SQL）或从文件读取（File）两种方式获取表清单
- ✅ **并发执行**：支持多线程并发执行 ANALYZE 命令，提高效率
- ✅ **批量写入**：使用批量插入和事务管理，优化写入性能
- ✅ **分页加载**：使用 ROW_NUMBER() 窗口函数实现分页，避免一次性加载过多数据，支持大规模表统计（2万+表）
- ✅ **异常处理**：完善的异常处理机制，单个表失败不影响整体流程
- ✅ **失败表记录**：自动记录执行失败的表名和日期，方便后续重跑
- ✅ **连接池管理**：使用 HikariCP 连接池，分离分析和写入连接池，支持连接超时配置
- ✅ **详细日志**：提供详细的执行日志，包括启动配置、任务进度、统计信息等，所有日志均为英文，避免乱码问题
- ✅ **灵活统计级别**：支持表级别和分区级别两种统计模式，可通过配置灵活切换，默认分区级别统计
- ✅ **SQL注入防护**：自动验证和清理表名，防止SQL注入攻击

## 收集的统计信息

- `numFiles`：文件数量
- `numRows`：记录数（行数）
- `totalSize`：总大小（字节）
- `rawDataSize`：原始数据大小（字节）

## 系统要求

- Java 8 或更高版本
- Maven 3.x
- Inceptor/Hive 数据库访问权限
- 能够执行 ANALYZE TABLE 命令的权限

## 项目结构

### 开发目录结构（源代码）

```
InceptorStatCollector/
├── src/
│   ├── main/
│   │   ├── java/io/transwarp/
│   │   │   ├── InceptorStatCollector.java    # 主程序类
│   │   │   └── ConfigUtil.java                # 配置工具类
│   │   └── resources/
│   │       ├── config.properties              # 配置文件模板
│   │       └── log4j.properties               # 日志配置文件
│   └── test/                                 # 测试代码目录
├── bin/                                      # 启动脚本目录
│   ├── start.sh                              # Linux/Mac 启动脚本
│   └── start.bat                             # Windows 启动脚本
├── libs/                                     # 依赖jar包目录（开发用）
│   └── inceptor-driver-8.37.3.jar            # Inceptor JDBC 驱动
├── conf/                                     # 配置文件目录（打包后自动创建）
│   └── config.properties                     # 运行时配置文件
├── pom.xml                                   # Maven 项目配置文件
└── README.md                                 # 项目说明文档
```

### 部署目录结构（打包后）

运行 `mvn clean package` 后，项目目录结构如下：

```
InceptorStatCollector/
├── bin/                                      # 启动脚本目录
│   ├── start.sh                              # Linux/Mac 启动脚本
│   └── start.bat                             # Windows 启动脚本
├── libs/                                     # 所有jar包目录（必需）
│   ├── InceptorStatCollector-1.0-SNAPSHOT.jar  # 主程序jar包
│   ├── inceptor-driver-8.37.3.jar            # Inceptor JDBC 驱动
│   ├── HikariCP-3.4.5.jar                   # 数据库连接池
│   ├── log4j-1.2.17.jar                     # 日志实现
│   ├── slf4j-api-1.7.25.jar                 # 日志API
│   └── slf4j-log4j12-1.7.25.jar             # SLF4J绑定
└── conf/                                     # 配置文件目录（必需）
    └── config.properties                     # 数据库连接和运行参数配置
```

### 目录详细说明

#### 📁 `bin/` - 启动脚本目录

包含用于启动程序的脚本文件：

| 文件 | 平台 | 说明 |
|------|------|------|
| `start.sh` | Linux/Mac | Bash脚本，自动设置UTF-8编码环境变量和Java参数 |
| `start.bat` | Windows | 批处理脚本，自动设置控制台代码页为UTF-8 |

**启动脚本功能：**
- ✅ 自动检查配置文件是否存在
- ✅ 自动检查jar包是否存在
- ✅ 自动构建classpath（包含libs目录下所有jar包）
- ✅ 设置UTF-8编码，解决中文乱码问题
- ✅ 设置Java运行时参数（`-Dfile.encoding=UTF-8`等）

**使用方法：**
```bash
# Linux/Mac
chmod +x bin/start.sh
bin/start.sh

# Windows
bin\start.bat
```

#### 📁 `libs/` - 依赖jar包目录

包含程序运行所需的所有jar包：

| 文件 | 说明 | 版本 |
|------|------|------|
| `InceptorStatCollector-1.0-SNAPSHOT.jar` | 主程序jar包 | 1.0-SNAPSHOT |
| `inceptor-driver-8.37.3.jar` | Inceptor JDBC驱动 | 8.37.3 |
| `HikariCP-3.4.5.jar` | 数据库连接池 | 3.4.5 |
| `log4j-1.2.17.jar` | 日志实现 | 1.2.17 |
| `slf4j-api-1.7.25.jar` | 日志API | 1.7.25 |
| `slf4j-log4j12-1.7.25.jar` | SLF4J绑定 | 1.7.25 |

**注意事项：**
- ⚠️ 所有jar包必须放在 `libs/` 目录下
- ⚠️ 启动脚本会自动扫描 `libs/` 目录下的所有jar包并添加到classpath
- ⚠️ 如果缺少jar包，程序将无法启动

#### 📁 `conf/` - 配置文件目录

包含程序运行时所需的配置文件：

| 文件 | 说明 | 必需 |
|------|------|------|
| `config.properties` | 数据库连接和运行参数配置 | ✅ 是 |
| `tables.txt` | 表清单文件（当 `table.source.mode=file` 时使用） | ⚠️ 条件必需 |
| `failed_tables.txt` | 失败表记录文件（自动生成） | ❌ 否 |

**配置文件说明：**

`config.properties` 包含以下配置项：

**数据库连接配置：**
```properties
jdbc.url=jdbc:hive2://host:port/database;transaction.type=inceptor;...
jdbc.driver=org.apache.hive.jdbc.HiveDriver
jdbc.username=your_username
jdbc.password=your_password
```

**目标表配置：**
```properties
inceptor.target.table.name=default.inceptor_stats_result_table
```

**性能参数配置：**
```properties
# 并发配置：并发执行 ANALYZE 的工作线程数
# - 值越大处理越快，但数据库压力越大
# - 建议范围：5-20，默认：10
# - 高性能数据库：15-20，共享数据库：5-10
concurrency.level=10

# 批量配置：批量写入的记录数
# - 值越大写入性能越好，但内存占用越大
# - 建议范围：200-1000，默认：500
# - 高数据量场景：500-1000，低内存环境：200-500
batch.size=500

# 结果队列容量：存储待写入统计结果的缓冲区大小
# - 建议为 batch.size 的 4-10 倍
# - 公式：result.queue.capacity ≥ batch.size × 4
# - batch.size=500 时，建议：2000-5000
result.queue.capacity=2000

# 分页配置：每次查询的表数量
# - 值越大查询次数越少，但内存占用越大
# - 建议范围：500-2000，默认：1000
# - 表数量多时用 1000-2000，表数量少时用 500-1000
page.size=1000

# 任务队列容量：存储待执行 ANALYZE 命令的队列大小
# - 必须大于等于总表数量
# - 建议为总表数量的 1.5-2 倍
# - 表数量 <10000：50000-100000，表数量 >10000：100000+
task.queue.capacity=100000
```

**统计级别配置：**
```properties
analyze.level=partition       # 统计级别: partition(分区级别,默认) 或 table(表级别)
```

| 配置值 | 说明 | 统计内容 | 使用场景 |
|--------|------|----------|----------|
| `partition` | 分区级别（默认） | 统计表级别和所有分区级别的统计信息 | 需要详细的分区级别统计信息，适合分区表较多的场景 |
| `table` | 表级别 | 只统计表级别的汇总统计信息（不包含分区信息） | 只需要表级别的汇总统计，减少数据量，提高处理速度 |

**统计级别详细说明：**

#### Partition 模式（分区级别，默认）

- **执行命令**：`ANALYZE TABLE table_name COMPUTE STATISTICS;`
- **统计内容**：
  - 表级别的汇总统计（一行记录，`partition_name` 为空）
  - 每个分区的详细统计（每个分区一行记录，`partition_name` 包含分区信息）
- **数据量**：对于有 N 个分区的表，会产生 N+1 条记录（1条表级别 + N条分区级别）
- **适用场景**：
  - 需要监控每个分区的数据变化
  - 需要分析分区级别的数据分布
  - 分区表较多且需要详细统计

**示例输出：**
```
表级别: default.test_table [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]
分区1:  default.test_table{dt=2023-01-01} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]
分区2:  default.test_table{dt=2023-01-02} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]
```

#### Table 模式（表级别）

- **执行命令**：`ANALYZE TABLE table_name COMPUTE STATISTICS;`（相同命令）
- **统计内容**：
  - 仅表级别的汇总统计（一行记录，`partition_name` 为空）
  - 自动过滤掉所有分区级别的统计结果
- **数据量**：每个表只产生 1 条记录
- **适用场景**：
  - 只需要表级别的汇总信息
  - 减少数据量，提高处理速度
  - 不需要分区级别的详细统计

**示例输出：**
```
表级别: default.test_table [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]
（分区级别的结果会被自动过滤）
```

**配置建议：**
- 如果表都是非分区表，两种模式效果相同
- 如果表都是分区表且需要详细统计，使用 `partition` 模式
- 如果只需要表级别汇总，使用 `table` 模式可以减少数据量
- 默认使用 `partition` 模式，保证统计信息的完整性

**查询SQL配置：**
```properties
table.query.sql=SELECT CONCAT(database_name,'.',table_name) FROM system.tables_v WHERE ...
```

**配置文件加载顺序：**
1. **优先**：`conf/config.properties`（外部配置文件，推荐）
2. **备选**：jar包内部的 `config.properties`（打包时包含的默认配置）

如果外部配置文件存在，会优先使用外部配置；如果不存在，则使用jar包内部的默认配置。

**配置文件位置：**
- 打包时，`src/main/resources/config.properties` 会自动复制到 `conf/config.properties`
- 可以直接编辑 `conf/config.properties` 进行配置
- 修改配置文件后无需重新打包，直接重启程序即可生效

### 目录创建说明

**打包后自动创建：**
- `conf/` 目录会在打包时自动创建
- `conf/config.properties` 会在打包时从 `src/main/resources/config.properties` 复制

**手动创建（如果需要）：**
```bash
# Linux/Mac
mkdir -p conf bin libs

# Windows
mkdir conf bin libs
```

### 目录结构总结

| 目录 | 必需 | 说明 | 创建方式 |
|------|------|------|----------|
| `bin/` | ✅ 是 | 启动脚本目录，包含 `start.sh` 和 `start.bat` | 源代码中已存在 |
| `libs/` | ✅ 是 | 所有jar包目录，包含主程序和所有依赖 | `mvn package` 自动创建 |
| `conf/` | ✅ 是 | 配置文件目录，包含 `config.properties` | `mvn package` 自动创建 |

**重要提示：**
- ⚠️ 运行程序前，必须确保 `bin/`、`libs/`、`conf/` 三个目录都存在
- ⚠️ `libs/` 目录必须包含所有必需的jar包（包括 `inceptor-driver-8.37.3.jar`）
- ⚠️ `conf/config.properties` 必须存在且配置正确
- ✅ 使用启动脚本（`bin/start.sh` 或 `bin/start.bat`）会自动检查这些要求

## 快速开始

### 1. 打包项目

```bash
mvn clean package
```

打包完成后，Maven会自动：
- ✅ 编译源代码并生成 `target/InceptorStatCollector-1.0-SNAPSHOT.jar`
- ✅ 复制所有依赖jar包到 `libs/` 目录
- ✅ 复制主程序jar包到 `libs/` 目录
- ✅ 创建 `conf/` 目录并复制配置文件模板到 `conf/config.properties`

**打包后的目录结构：**
```
InceptorStatCollector/
├── bin/                    # 启动脚本（已存在）
├── libs/                   # 所有jar包（打包后自动生成）
│   ├── InceptorStatCollector-1.0-SNAPSHOT.jar
│   ├── inceptor-driver-8.37.3.jar
│   ├── HikariCP-3.4.5.jar
│   ├── log4j-1.2.17.jar
│   ├── slf4j-api-1.7.25.jar
│   └── slf4j-log4j12-1.7.25.jar
└── conf/                   # 配置文件（打包后自动生成）
    └── config.properties
```

### 2. 配置数据库连接

**重要**：配置文件支持两种方式：

#### 方式一：外部配置文件（推荐）

**前置条件：**
- ⚠️ Inceptor JDBC驱动 `inceptor-driver-8.37.3.jar` 需要提前自行放到 `libs/` 目录下
- ✅ 打包时，`conf/config.properties` 会自动创建

**配置文件位置：**
```
InceptorStatCollector/
├── bin/
│   ├── start.sh
│   └── start.bat
├── libs/
│   ├── InceptorStatCollector-1.0-SNAPSHOT.jar
│   ├── inceptor-driver-8.37.3.jar    ← 需要手动放置
│   └── (其他依赖jar包)
└── conf/
    └── config.properties              ← 外部配置文件（推荐）
```

**优点**：
- ✅ 修改配置无需重新打包
- ✅ 不同环境可以使用不同配置文件
- ✅ 配置管理更方便
- ✅ 打包后自动创建配置文件模板

#### 方式二：jar包内部配置

如果 `conf/config.properties` 外部配置文件不存在，程序会自动从jar包内部加载 `config.properties`。

**注意**：jar包内部配置是只读的，无法修改，建议使用外部配置文件。

**创建外部配置文件**：

打包完成后，配置文件会自动复制到 `conf/config.properties`，编辑该文件即可：

```properties
# 数据库连接配置
jdbc.url=jdbc:hive2://your_host:10000/default;transaction.type=inceptor;hive.server2.idle.session.timeout=64800000;hive.server2.idle.operation.timeout=3600000
jdbc.driver=org.apache.hive.jdbc.HiveDriver
jdbc.username=your_username
jdbc.password=your_password

#====================== 目标表配置 ======================
#用于存储统计信息的目标表名称，格式：数据库名.表名
#- 若该表不存在，程序将自动创建
#- 表结构会根据 analyze.level 配置项的取值不同而有所差异
#- 格式示例：数据库名。表名（如 default.inceptor_stats_result_table）
#- 程序运行前，对应的数据库必须已存在
#- 该表采用 ORC 格式存储，且支持事务功能
#- 表按 data_day 字段进行分区（分区粒度为月度）
#- 表按 id 字段分桶，共分为 11 个桶，以提升查询性能
#- 可针对不同环境（开发、测试、生产）配置不同的表名
#- 示例：prod.inceptor_stats_result_table、test.inceptor_stats_result_table
inceptor.target.table.name=default.inceptor_stats_result_table

# 并发配置：并发执行 ANALYZE 的工作线程数
# - 值越大处理越快，但数据库压力越大
# - 建议范围：5-20，默认：10
# - 高性能数据库：15-20，共享数据库：5-10
concurrency.level=10

# 批量配置：批量写入的记录数
# - 值越大写入性能越好，但内存占用越大
# - 建议范围：200-1000，默认：500
# - 高数据量场景：500-1000，低内存环境：200-500
batch.size=500

# 结果队列容量：存储待写入统计结果的缓冲区大小
# - 建议为 batch.size 的 4-10 倍
# - 公式：result.queue.capacity ≥ batch.size × 4
# - batch.size=500 时，建议：2000-5000
result.queue.capacity=2000

# 分页配置：每次查询的表数量
# - 值越大查询次数越少，但内存占用越大
# - 建议范围：500-2000，默认：1000
# - 表数量多时用 1000-2000，表数量少时用 500-1000
page.size=1000

# 任务队列容量：存储待执行 ANALYZE 命令的队列大小
# - 必须大于等于总表数量
# - 建议为总表数量的 1.5-2 倍
# - 表数量 <10000：50000-100000，表数量 >10000：100000+
task.queue.capacity=100000

# 超时配置
#====================== 超时配置 ======================
#等待所有分析任务完成的超时时间（单位：小时）
#- 控制程序等待所有 ANALYZE 命令执行完毕的时长
#- 若达到超时时间，程序会记录一条警告日志，但继续执行后续流程
#- 建议配置范围：1-24 小时，默认值：2 小时
#- 适用于数据表数量 < 1000 的系统：1-2 小时
#- 适用于数据表数量 1000-10000 的系统：2-4 小时
#- 适用于数据表数量 > 10000 的系统：4-8 小时或更长
#- 配置为 0 或负数时，程序会无限等待（不推荐此配置）
#- 应根据数据表总数及单表复杂度调整该参数

analysis.task.timeout.hours=2

#等待批量写入线程完成的超时时间（单位：分钟）
#- 控制程序等待批量写入器完成所有数据写入的时长
#- 若达到超时时间，程序会记录一条警告日志，但继续执行后续流程
#- 建议配置范围：10-120 分钟，默认值：30 分钟
#- 适用于小数据量场景：15-30 分钟
#- 适用于中等数据量场景：30-60 分钟
#- 适用于大数据量场景：60-120 分钟
#- 该参数值应小于 analysis.task.timeout.hours 对应的分钟数
#- 示例：若 analysis.task.timeout.hours = 2，则此参数值应小于 120 分钟
batch.writer.timeout.minutes=30

# 统计级别配置
# partition: 统计表级别和所有分区级别的统计信息（默认）
# table: 只统计表级别的汇总统计信息（不包含分区信息）
analyze.level=table

#====================== 数据表来源配置 ======================
#数据表来源模式：sql（从数据库查询，默认模式）或 file（从文件读取）
#sql 模式：使用 table.query.sql 配置的语句从 system.tables_v 中查询数据表
#file 模式：从 table.list.file 配置的文件中读取数据表列表
table.source.mode=sql

# ====================== Query SQL Configuration ======================
# SQL query for getting table list from system.tables_v (used when table.source.mode=sql)
# - 该 SQL 用于查询所有需要采集统计信息的数据表
# - 必须返回单列结果，且列中数据表名称格式为：数据库名.表名
# - 程序会自动为每一张匹配的数据表生成对应的 ANALYZE 命令
# - 可自定义 WHERE 子句，用于排除特定类型的数据表
# - 常见的排除类型：hyperbase 表、scope 表、hyperdrive 表等
# - 也可通过在 NOT IN 子句中添加数据库名，排除指定的数据库
# - SQL 语法必须与 Inceptor/Hive 兼容
# - 示例：若要排除 test 数据库，可添加条件：AND database_name not in ('system', 'test')
# - 示例：若要仅包含指定数据库，可使用条件：AND database_name in ('db1', 'db2')
table.query.sql=SELECT CONCAT(database_name,'.',table_name) \
  FROM system.tables_v \
  WHERE table_format NOT IN ('io.transwarp.scope.ScopeInputFormat','io.transwarp.hyper2drive.HyperdriveInputFormat','hbase','hyperdrive','org.apache.hadoop.hive.ql.io.tdt.refactor.JDBCDBInputFormat') \
    AND database_name not in ('system')

#====================== 数据表清单文件配置 ======================
#数据表清单文件路径（仅在 table.source.mode = file 时生效）
#文件格式：每行一个数据表，格式为：数据库名.表名
#支持空行和注释行（以 # 开头的行）
#文件位置：
#  - 相对路径（如 tables.txt）：文件放在程序运行的当前工作目录下（通常与 conf/ 目录同级）
#  - 相对路径（如 conf/tables.txt）：文件放在 conf/ 目录下
#  - 绝对路径（如 /opt/inceptor-stat-collector/tables.txt）：文件放在指定绝对路径
#示例：
## 这是一条注释
#default.table1
#default.table2
#prod.user_table
table.list.file=tables.txt

#====================== 失败表记录配置 ======================
#失败表记录文件路径（记录执行失败的表名和日期）
#文件格式：每行一条记录，格式为：表名,失败日期（例如：db.table,2025-12-16）
#该文件可以作为 table.list.file 的输入，通过 table.source.mode=file 模式重跑失败的表
#提取表名：可以使用第一列（表名）创建新的表清单文件
#默认：conf/failed_tables.txt
failed.tables.file=conf/failed_tables.txt
```

### 3. 运行程序

#### 方式一：使用启动脚本（推荐）

**Windows:**
```bash
bin\start.bat
```

**Linux/Mac:**
```bash
chmod +x bin/start.sh
bin/start.sh
```

#### 方式二：手动运行

**Windows:**
```bash
java -cp "target\InceptorStatCollector-1.0-SNAPSHOT.jar;target\lib\*;libs\inceptor-driver-8.37.3.jar" io.transwarp.InceptorStatCollector
```

**Linux/Mac:**
```bash
java -cp "target/InceptorStatCollector-1.0-SNAPSHOT.jar:target/lib/*:libs/inceptor-driver-8.37.3.jar" io.transwarp.InceptorStatCollector
```

#### 方式三：使用 Maven 运行（开发环境）

```bash
mvn exec:java
```

**注意**：使用 Maven 运行时，配置文件会从 `src/main/resources/config.properties` 加载。

## 配置说明

### 数据库连接配置

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `jdbc.url` | Inceptor/Hive JDBC 连接 URL | `jdbc:hive2://host:port/database` |
| `jdbc.driver` | JDBC 驱动类名 | `org.apache.hive.jdbc.HiveDriver` |
| `jdbc.username` | 数据库用户名 | `hive` |
| `jdbc.password` | 数据库密码 | `password` |

### 性能配置

| 配置项 | 说明 | 默认值 | 建议值 | 详细说明 |
|--------|------|--------|--------|----------|
| `concurrency.level` | 并发执行 ANALYZE 的工作线程数 | 10 | 5-20 | **作用**：控制同时执行 ANALYZE 命令的线程数量<br>**影响**：值越大，处理速度越快，但数据库压力越大<br>**调优**：高性能数据库可设为 15-20，共享数据库建议 5-10<br>**注意**：每个线程会占用一个数据库连接 |
| `batch.size` | 批量写入的记录数 | 500 | 200-1000 | **作用**：控制每次批量写入目标表的记录数<br>**影响**：值越大，写入性能越好，但内存占用越大<br>**调优**：高数据量场景用 500-1000，低内存环境用 200-500<br>**注意**：需要根据可用内存和网络延迟调整 |
| `result.queue.capacity` | 结果队列容量 | 2000 | batch.size 的 4-10 倍 | **作用**：结果队列的缓冲区大小，用于存储待写入的统计结果<br>**影响**：值太小会导致工作线程阻塞，值太大会占用过多内存<br>**调优**：建议为 batch.size 的 4-10 倍<br>**公式**：result.queue.capacity ≥ batch.size × 4<br>**示例**：batch.size=500 时，建议设为 2000-5000 |
| `page.size` | 分页查询大小 | 1000 | 500-2000 | **作用**：控制每次从 system.tables_v 查询的表数量<br>**影响**：值越大，查询次数越少但内存占用越大<br>**调优**：表数量 >10000 时用 1000-2000，<1000 时用 500-1000<br>**注意**：应根据总表数量调整 |
| `task.queue.capacity` | 任务队列容量 | 100000 | ≥ 总表数 | **作用**：存储待处理的 ANALYZE 命令队列大小<br>**影响**：值太小会导致任务加载阻塞<br>**调优**：至少等于总表数量<br>**公式**：task.queue.capacity ≥ 总表数<br>**示例**：<10000 表用 10000-50000，>10000 表用 100000+ |

**性能配置详细说明：**

#### 1. concurrency.level（并发级别）

**工作原理：**
- 程序会创建指定数量的工作线程，每个线程独立执行 ANALYZE 命令
- 每个线程维护自己的数据库连接，从任务队列获取任务并执行
- 多个线程并发执行，提高整体处理速度

**调优建议：**
- **高性能数据库**（独立服务器，性能好）：15-20
- **中等性能数据库**（共享服务器）：10-15
- **低性能数据库**（资源受限）：5-10
- **测试环境**：建议从 5 开始，逐步增加

**注意事项：**
- 值过高可能导致数据库连接数过多，影响数据库性能
- 值过低会导致处理速度慢，无法充分利用数据库资源
- 建议根据数据库服务器的 CPU 核心数和性能调整

#### 2. batch.size（批量大小）

**工作原理：**
- 批量写入线程会收集统计结果，达到 batch.size 数量后一次性写入数据库
- 使用批量插入（batch insert）提高写入性能
- 减少数据库交互次数，提高整体吞吐量

**调优建议：**
- **高数据量场景**（每天产生大量统计结果）：500-1000
- **中等数据量场景**：300-500
- **低数据量场景**（或低内存环境）：200-300
- **网络延迟高**：建议使用较大的批量大小（500-1000）

**注意事项：**
- 批量大小过大会导致单次写入时间过长，影响实时性
- 批量大小过小会增加数据库交互次数，降低性能
- 需要根据可用内存调整，避免内存溢出

#### 3. result.queue.capacity（结果队列容量）

**工作原理：**
- 结果队列是工作线程和写入线程之间的缓冲区
- 工作线程将解析后的统计结果放入队列
- 写入线程从队列取出结果进行批量写入

**调优建议：**
- **计算公式**：result.queue.capacity ≥ batch.size × 4
- **推荐倍数**：4-10 倍 batch.size
- **示例**：
  - batch.size=200 → result.queue.capacity=800-2000
  - batch.size=500 → result.queue.capacity=2000-5000
  - batch.size=1000 → result.queue.capacity=4000-10000

**注意事项：**
- 容量太小会导致工作线程频繁等待，降低并发效率
- 容量太大占用内存，但不会显著提升性能
- 建议设置为 batch.size 的 4-10 倍

#### 4. page.size（分页大小）

**工作原理：**
- 程序使用分页查询从 system.tables_v 获取表列表
- 每次查询返回 page.size 条记录
- 使用 ROW_NUMBER() 窗口函数实现分页（兼容 Hive/Inceptor）

**调优建议：**
- **表数量 < 1000**：500-1000
- **表数量 1000-10000**：1000-1500
- **表数量 > 10000**：1500-2000
- **表数量非常大（>50000）**：2000-3000

**注意事项：**
- 分页大小过大会增加单次查询时间，但减少查询次数
- 分页大小过小会增加查询次数，但单次查询更快
- 需要根据总表数量和数据库性能平衡
- **分页判断逻辑**：程序基于数据库实际返回的记录数判断是否还有更多数据
  - 如果返回记录数等于 `page.size`，继续查询下一页
  - 如果返回记录数小于 `page.size`，表示已经是最后一页
- **重要**：即使某些记录验证失败被跳过，程序也能正确判断是否还有更多数据，确保加载所有表

#### 5. task.queue.capacity（任务队列容量）

**工作原理：**
- 任务队列存储所有待执行的 ANALYZE 命令
- 程序启动时会将所有表的 ANALYZE 命令加载到队列中
- 工作线程从队列中获取任务并执行

**调优建议：**
- **表数量 < 1000**：10000-20000（留有余量）
- **表数量 1000-10000**：50000-100000
- **表数量 > 10000**：100000-500000
- **表数量非常大（>50000）**：500000 或更大

**注意事项：**
- 容量必须大于等于总表数量，否则会导致任务加载阻塞
- 建议设置为总表数量的 1.5-2 倍，留有余量
- 容量过大不会显著影响性能，但会占用少量内存
- **重要**：如果表数量很大（>10000），必须确保队列容量足够大，否则 `queue.put()` 会阻塞等待，影响分页加载
- 如果队列满了，任务加载线程会等待工作线程处理，这是正常现象，不会导致数据丢失

### 超时配置

| 配置项 | 说明 | 默认值 | 单位 | 建议值 | 详细说明 |
|--------|------|--------|------|--------|----------|
| `analysis.task.timeout.hours` | 等待所有分析任务完成的超时时间 | 2 | 小时 | 1-24 | **作用**：控制程序等待所有 ANALYZE 命令完成的最大时间<br>**影响**：超时后会记录警告但继续执行，不会中断程序<br>**调优**：根据表数量和复杂度调整<br>**注意**：设置为 0 或负数会无限等待（不推荐） |
| `batch.writer.timeout.minutes` | 等待批量写入线程完成的超时时间 | 30 | 分钟 | 10-120 | **作用**：控制程序等待批量写入线程完成的最大时间<br>**影响**：超时后会记录警告但继续执行<br>**调优**：根据数据量调整，应小于 analysis.task.timeout.hours<br>**注意**：建议小于 analysis.task.timeout.hours 的对应分钟数 |

### 失败表记录配置

| 配置项 | 说明 | 默认值 | 详细说明 |
|--------|------|--------|----------|
| `failed.tables.file` | 失败表记录文件路径 | `conf/failed_tables.txt` | **作用**：记录执行失败的表名和日期<br>**格式**：每行一条记录，格式为 `table_name,failed_date`（例如：`db.table,2025-12-16`）<br>**用途**：可以提取表名作为 `table.list.file` 的输入，通过 `table.source.mode=file` 模式重跑失败的表<br>**文件位置**：可以使用相对路径或绝对路径<br>**注意**：文件会自动创建，如果目录不存在会自动创建目录 |

| 配置项 | 说明 | 默认值 | 详细说明 |
|--------|------|--------|----------|
| `failed.tables.file` | 失败表记录文件路径 | `conf/failed_tables.txt` | **作用**：记录执行失败的表名和日期<br>**格式**：每行一条记录，格式为 `table_name,failed_date`（例如：`db.table,2025-12-16`）<br>**用途**：可以提取表名作为 `table.list.file` 的输入，通过 `table.source.mode=file` 模式重跑失败的表<br>**文件位置**：可以使用相对路径或绝对路径<br>**注意**：文件会自动创建，如果目录不存在会自动创建目录 |

**超时配置详细说明：**

#### 1. analysis.task.timeout.hours（分析任务超时时间）

**工作原理：**
- 程序启动后，会创建多个工作线程并发执行 ANALYZE 命令
- 主线程会等待所有工作线程完成所有任务
- 如果超过此时间仍有任务未完成，程序会记录警告并继续执行后续步骤

**调优建议：**
- **表数量 < 1000**：1-2 小时
- **表数量 1000-10000**：2-4 小时
- **表数量 > 10000**：4-8 小时或更长
- **表数量非常大（>50000）**：8-24 小时

**影响因素：**
- 表数量：表越多，需要的时间越长
- 表大小：大表需要更长的 ANALYZE 时间
- 分区数量：分区表需要更多时间
- 数据库性能：数据库性能越好，处理越快
- 并发级别：并发级别越高，完成越快

**注意事项：**
- 如果设置为 0 或负数，程序会一直等待直到所有任务完成（不推荐，可能导致程序挂起）
- 超时不会中断程序执行，只是记录警告日志
- 建议先使用默认值运行一次，根据实际执行时间调整
- 对于生产环境，建议设置较长的超时时间，避免因网络波动等原因导致任务中断

**示例场景：**
```
场景1：1000 张表，每张表平均 10 个分区
- 预计时间：1-2 小时
- 建议配置：analysis.task.timeout.hours=2

场景2：10000 张表，每张表平均 50 个分区
- 预计时间：4-6 小时
- 建议配置：analysis.task.timeout.hours=6

场景3：50000 张表，每张表平均 100 个分区
- 预计时间：12-24 小时
- 建议配置：analysis.task.timeout.hours=24
```

#### 2. batch.writer.timeout.minutes（批量写入超时时间）

**工作原理：**
- 程序使用单独的线程进行批量写入操作
- 主线程在发送终止信号后，会等待写入线程完成所有数据的写入
- 如果超过此时间写入线程仍未完成，程序会记录警告

**调优建议：**
- **数据量较小**（<100万条记录）：15-30 分钟
- **数据量中等**（100万-1000万条记录）：30-60 分钟
- **数据量较大**（1000万-1亿条记录）：60-120 分钟
- **数据量非常大**（>1亿条记录）：120-240 分钟

**影响因素：**
- 数据量：记录数越多，写入时间越长
- 批量大小：batch.size 越大，写入越快但单次写入时间越长
- 数据库写入性能：数据库写入性能越好，完成越快
- 网络延迟：网络延迟越高，写入越慢

**注意事项：**
- 此值应该小于 `analysis.task.timeout.hours` 的对应分钟数
- 例如：如果 `analysis.task.timeout.hours=2`（120分钟），此值应该 < 120 分钟
- 超时不会中断程序执行，只是记录警告日志
- 如果经常超时，可以增加此值或减少 `batch.size`

**计算公式：**
```
batch.writer.timeout.minutes < analysis.task.timeout.hours × 60

示例：
- analysis.task.timeout.hours=2 → batch.writer.timeout.minutes < 120
- analysis.task.timeout.hours=4 → batch.writer.timeout.minutes < 240
```

**示例场景：**
```
场景1：1000 张表，分区级别统计，平均每张表 10 个分区
- 预计记录数：1000 × (10+1) = 11000 条
- 预计写入时间：5-10 分钟
- 建议配置：batch.writer.timeout.minutes=30

场景2：10000 张表，表级别统计
- 预计记录数：10000 条
- 预计写入时间：10-20 分钟
- 建议配置：batch.writer.timeout.minutes=30

场景3：10000 张表，分区级别统计，平均每张表 100 个分区
- 预计记录数：10000 × (100+1) = 1010000 条
- 预计写入时间：30-60 分钟
- 建议配置：batch.writer.timeout.minutes=60
```

### 目标表配置

| 配置项 | 说明 | 示例 | 详细说明 |
|--------|------|------|----------|
| `inceptor.target.table.name` | 存储统计结果的目标表名 | `default.inceptor_stats_result_table` | **作用**：指定存储统计结果的目标表<br>**格式**：`database.table`（数据库名.表名）<br>**自动创建**：表不存在时会自动创建<br>**表结构**：根据 `analyze.level` 动态生成<br>**存储格式**：ORC 格式，支持事务 |

**目标表配置详细说明：**

#### inceptor.target.table.name（目标表名）

**工作原理：**
- 程序启动时会检查目标表是否存在
- 如果表不存在，程序会自动创建表（使用 `CREATE TABLE IF NOT EXISTS`）
- 表结构会根据 `analyze.level` 配置动态生成
- 所有统计结果都会写入此表

**表名格式：**
- **格式要求**：`database.table`（数据库名.表名）
- **示例**：
  - `default.inceptor_stats_result_table`（默认数据库）
  - `stats_db.inceptor_stats_result_table`（自定义数据库）
  - `prod.inceptor_stats_result_table`（生产环境）
  - `test.inceptor_stats_result_table`（测试环境）

**表结构说明：**

**分区级别模式（analyze.level=partition）的表结构：**
```sql
CREATE TABLE IF NOT EXISTS default.inceptor_stats_result_table (
    id STRING COMMENT '唯一标识（UUID）',
    data_time STRING COMMENT '插入时间（格式：yyyy-MM-dd HH:mm:ss）',
    data_day DATE COMMENT '分区时间（用于表分区）',
    table_name STRING COMMENT '表名（格式：database.table）',
    partition_name STRING COMMENT '分区名称（表级别时为空字符串）',
    is_partition INT COMMENT '是否分区表（0=否，1=是）',
    numFiles INT COMMENT '文件数',
    numRows INT COMMENT '行数',
    totalSize BIGINT COMMENT '总大小（字节）',
    rawDataSize BIGINT COMMENT '原始数据大小（字节）'
)
COMMENT '存放inceptor每张表的统计信息'
PARTITIONED BY RANGE (data_day)
INTERVAL (numtoyminterval('1','month'))
(
  PARTITION p VALUES LESS THAN ('1970-01-01')
)
CLUSTERED BY (id) INTO 11 BUCKETS STORED AS ORC
TBLPROPERTIES('transactional'='true')
```

**表级别模式（analyze.level=table）的表结构：**
```sql
CREATE TABLE IF NOT EXISTS default.inceptor_stats_result_table (
    id STRING COMMENT '唯一标识（UUID）',
    data_time STRING COMMENT '插入时间（格式：yyyy-MM-dd HH:mm:ss）',
    data_day DATE COMMENT '分区时间（用于表分区）',
    table_name STRING COMMENT '表名（格式：database.table）',
    -- 注意：不包含 partition_name 和 is_partition 字段
    numFiles INT COMMENT '文件数',
    numRows INT COMMENT '行数',
    totalSize BIGINT COMMENT '总大小（字节）',
    rawDataSize BIGINT COMMENT '原始数据大小（字节）'
)
COMMENT '存放inceptor每张表的统计信息'
PARTITIONED BY RANGE (data_day)
INTERVAL (numtoyminterval('1','month'))
(
  PARTITION p VALUES LESS THAN ('1970-01-01')
)
CLUSTERED BY (id) INTO 11 BUCKETS STORED AS ORC
TBLPROPERTIES('transactional'='true')
```

**表特性说明：**

1. **自动创建**：
   - 程序启动时会自动检查表是否存在
   - 如果表不存在，会自动创建表
   - 如果表已存在，会验证表结构是否匹配当前配置
   - 表结构会根据 `analyze.level` 配置动态调整

2. **存储格式**：
   - **ORC 格式**：列式存储，压缩率高，查询性能好
   - **事务支持**：`transactional='true'`，支持 ACID 事务
   - **分桶存储**：按 `id` 字段分桶（11个桶），提高查询性能

3. **分区策略**：
   - **分区字段**：`data_day`（DATE 类型）
   - **分区类型**：范围分区（RANGE PARTITION）
   - **分区间隔**：每月一个分区（`INTERVAL (numtoyminterval('1','month'))`）
   - **自动分区**：根据插入数据的 `data_day` 值自动创建分区
   - **优势**：按时间分区便于数据管理和查询，可以快速删除历史数据

4. **字段说明**：
   - `id`：每条记录的唯一标识（UUID）
   - `data_time`：记录插入时间（字符串格式：yyyy-MM-dd HH:mm:ss）
   - `data_day`：分区时间（DATE 类型，用于表分区）
   - `table_name`：被统计的表名（格式：database.table）
   - `partition_name`：分区名称（仅分区级别模式，表级别时为空）
   - `is_partition`：是否为分区表（仅分区级别模式，0=否，1=是）
   - `numFiles`：文件数量
   - `numRows`：行数
   - `totalSize`：总大小（字节）
   - `rawDataSize`：原始数据大小（字节）

**配置建议：**

1. **数据库选择**：
   - 建议使用独立的数据库存储统计结果，避免与业务数据混在一起
   - 示例：`stats_db.inceptor_stats_result_table`
   - 如果使用默认数据库，确保有足够的权限

2. **表名命名**：
   - 建议使用有意义的表名，便于识别和管理
   - 可以按环境区分：`prod.inceptor_stats_result`, `test.inceptor_stats_result`
   - 可以按时间区分：`inceptor_stats_result_2024`, `inceptor_stats_result_2025`

3. **权限要求**：
   - 程序需要对目标数据库有 CREATE TABLE 权限
   - 程序需要对目标表有 INSERT 权限
   - 如果表已存在，需要有 SELECT 权限（用于验证表结构）

4. **表管理**：
   - 表会自动按月分区，便于数据管理
   - 可以定期删除历史分区：`ALTER TABLE table_name DROP PARTITION (data_day < '2024-01-01')`
   - 建议定期清理历史数据，避免表过大影响性能

**注意事项：**

1. **数据库必须存在**：
   - 目标数据库必须在程序运行前存在
   - 如果数据库不存在，程序会报错并退出
   - 可以使用 `CREATE DATABASE IF NOT EXISTS database_name;` 创建数据库

2. **表结构匹配**：
   - 如果表已存在，程序会验证表结构是否匹配当前配置
   - 如果表结构不匹配（例如字段数量不同），程序会报错
   - 建议在切换统计级别前，先删除旧表或使用新的表名

3. **表名格式**：
   - 必须使用 `database.table` 格式，不能只写表名
   - 表名不能包含特殊字符（建议使用字母、数字、下划线）
   - 表名区分大小写（取决于数据库配置）

4. **并发写入**：
   - 程序使用事务保证数据一致性
   - 多个程序实例可以同时写入同一张表（不推荐）
   - 建议同一时间只有一个程序实例运行

5. **数据量管理**：
   - 表会随时间增长，建议定期清理历史数据
   - 可以使用分区删除快速清理历史数据
   - 建议保留最近 3-6 个月的数据

**示例场景：**

**场景1：使用默认数据库**
```properties
inceptor.target.table.name=default.inceptor_stats_result_table
```
- 表创建在默认数据库中
- 适合测试环境或小规模使用

**场景2：使用独立数据库**
```properties
inceptor.target.table.name=stats_db.inceptor_stats_result_table
```
- 表创建在独立的统计数据库中
- 适合生产环境，便于管理和维护

**场景3：按环境区分**
```properties
# 生产环境
inceptor.target.table.name=prod.inceptor_stats_result_table

# 测试环境
inceptor.target.table.name=test.inceptor_stats_result_table
```
- 不同环境使用不同的表
- 避免测试数据污染生产数据

**场景4：按时间区分**
```properties
# 2024年数据
inceptor.target.table.name=stats_db.inceptor_stats_result_2024

# 2025年数据
inceptor.target.table.name=stats_db.inceptor_stats_result_2025
```
- 按年度创建不同的表
- 便于数据归档和管理

### 统计级别配置

| 配置项 | 说明 | 默认值 | 可选值 |
|--------|------|--------|--------|
| `analyze.level` | 统计级别 | `partition` | `partition`（分区级别）或 `table`（表级别） |

**详细说明请参考上面的"统计级别配置"章节。**

### 表清单来源配置

| 配置项 | 说明 | 默认值 | 可选值 | 详细说明 |
|--------|------|--------|--------|----------|
| `table.source.mode` | 表清单来源模式 | `sql` | `sql`（从数据库查询）或 `file`（从文件读取） | **作用**：选择获取表清单的方式<br>**sql**：使用 `table.query.sql` 从数据库查询表列表<br>**file**：从 `table.list.file` 指定的文件读取表列表 |

### 查询SQL配置（table.source.mode=sql 时使用）

| 配置项 | 说明 | 详细说明 |
|--------|------|----------|
| `table.query.sql` | 用于查询需要统计的表的 SQL 语句 | **作用**：从系统表查询需要统计的表列表<br>**要求**：必须返回单列，格式为 `database.table`<br>**用途**：程序会根据查询结果自动生成 ANALYZE 命令<br>**自定义**：可以通过 WHERE 子句过滤表类型或数据库<br>**注意**：仅在 `table.source.mode=sql` 时生效 |

**查询SQL配置详细说明：**

#### table.query.sql（表查询SQL）

**工作原理：**
- 程序启动时会执行此 SQL 查询，从 `system.tables_v` 系统表获取需要统计的表列表
- SQL 必须返回单列，每行是一个表名，格式为：`database.table`
- 程序会为每个表自动生成 `ANALYZE TABLE database.table COMPUTE STATISTICS;` 命令
- 使用分页查询（`page.size`）避免一次性加载过多数据

**SQL 要求：**
1. **返回列数**：必须返回单列（表名）
2. **表名格式**：格式必须为 `database.table`（使用 `CONCAT(database_name,'.',table_name)`）
3. **SQL 语法**：必须符合 Inceptor/Hive SQL 语法规范
4. **性能考虑**：建议添加适当的 WHERE 条件过滤不需要的表

**默认 SQL 说明：**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE table_format NOT IN (
    'io.transwarp.scope.ScopeInputFormat',
    'io.transwarp.hyper2drive.HyperdriveInputFormat',
    'hbase',
    'hyperdrive',
    'org.apache.hadoop.hive.ql.io.tdt.refactor.JDBCDBInputFormat'
) 
AND database_name not in ('system')
```

**默认 SQL 排除的表类型：**
- `ScopeInputFormat`：Scope 表
- `HyperdriveInputFormat`：Hyperdrive 表
- `hbase`：HBase 表
- `hyperdrive`：Hyperdrive 表
- `JDBCDBInputFormat`：JDBC 外部表
- `system` 数据库：系统数据库

**自定义 SQL 示例：**

**示例1：排除特定数据库**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE table_format NOT IN ('hbase','hyperdrive')
  AND database_name not in ('system', 'test', 'temp')
```

**示例2：只包含特定数据库**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE database_name in ('production', 'staging')
  AND table_format NOT IN ('hbase','hyperdrive')
```

**示例3：排除特定表名模式**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE table_format NOT IN ('hbase','hyperdrive')
  AND database_name not in ('system')
  AND table_name NOT LIKE '%_temp%'
  AND table_name NOT LIKE '%_bak%'
```

**示例4：只统计分区表**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE table_format NOT IN ('hbase','hyperdrive')
  AND database_name not in ('system')
  AND partition_count > 0
```

**示例5：排除视图**
```sql
SELECT CONCAT(database_name,'.',table_name) 
FROM system.tables_v 
WHERE table_format NOT IN ('hbase','hyperdrive')
  AND database_name not in ('system')
  AND table_type = 'TABLE'  -- 排除视图
```

**调优建议：**
1. **排除不需要的表类型**：
   - 在 `NOT IN` 子句中添加不需要的表格式
   - 常见排除：hyperbase、scope、hyperdrive、hbase 等

2. **排除特定数据库**：
   - 在 `database_name not in (...)` 中添加不需要的数据库
   - 常见排除：system、test、temp、backup 等

3. **只包含特定数据库**：
   - 使用 `database_name in (...)` 只查询需要的数据库
   - 适用于只需要统计部分数据库的场景

4. **性能优化**：
   - 添加适当的 WHERE 条件减少查询结果
   - 避免使用复杂的 JOIN 或子查询
   - 确保 SQL 能够利用索引（如果 system.tables_v 有索引）

**注意事项：**
- SQL 必须返回单列，否则程序会报错
- 表名格式必须为 `database.table`，不能只是表名
- 如果 SQL 语法错误，程序启动时会报错并退出
- 修改 SQL 后需要重启程序才能生效
- 建议先在数据库中测试 SQL 是否正确，再配置到文件中
- 如果查询结果为空，程序会正常退出（没有表需要统计）

**常见问题：**

**问题1：SQL 返回多列**
- **错误**：`SELECT database_name, table_name FROM system.tables_v`
- **正确**：`SELECT CONCAT(database_name,'.',table_name) FROM system.tables_v`

**问题2：表名格式不正确**
- **错误**：`SELECT table_name FROM system.tables_v`（只有表名，没有数据库名）
- **正确**：`SELECT CONCAT(database_name,'.',table_name) FROM system.tables_v`

**问题3：包含不需要的表**
- **解决**：在 WHERE 子句中添加过滤条件，排除不需要的表类型或数据库

**问题4：SQL 语法错误**
- **解决**：先在数据库中测试 SQL，确保语法正确后再配置

### 表清单文件配置（table.source.mode=file 时使用）

| 配置项 | 说明 | 默认值 | 详细说明 |
|--------|------|--------|----------|
| `table.list.file` | 表清单文件路径 | `tables.txt` | **作用**：指定包含表清单的文件路径<br>**格式**：每行一个表名，格式为 `database.table`<br>**支持**：空行和注释（以 `#` 开头的行）<br>**注意**：仅在 `table.source.mode=file` 时生效 |

**表清单文件配置详细说明：**

#### table.list.file（表清单文件）

**工作原理：**
- 当 `table.source.mode=file` 时，程序会从指定文件读取表清单
- 文件格式：每行一个表名，格式为 `database.table`
- 支持空行和注释（以 `#` 开头的行会被忽略）
- 程序会为每个表自动生成 `ANALYZE TABLE database.table COMPUTE STATISTICS;` 命令
- 表名会经过格式验证，不符合格式的表会被跳过并记录警告

**文件格式要求：**
1. **表名格式**：必须是 `database.table` 格式（使用点号分隔）
2. **编码**：文件必须使用 UTF-8 编码
3. **注释**：以 `#` 开头的行会被忽略
4. **空行**：空行会被忽略
5. **路径**：可以使用相对路径或绝对路径

**文件示例：**
```text
# This is a comment line
# Table list file for statistics collection

# Production tables
prod.user_table
prod.order_table
prod.product_table

# Test tables
test.user_table
test.order_table

# Default database tables
default.log_table
default.config_table
```

**文件位置说明：**

**重要**：`tables.txt` 文件的位置取决于配置中的路径设置：

1. **相对路径**（推荐）：
   - 如果配置为 `table.list.file=tables.txt`（相对路径）
   - 文件应放在**程序运行的当前工作目录**下
   - 通常与 `conf/`、`bin/`、`libs/` 目录同级
   - 示例目录结构：
     ```
     InceptorStatCollector/
     ├── bin/
     ├── libs/
     ├── conf/
     │   └── config.properties
     └── tables.txt          ← 文件放在这里（项目根目录）
     ```

2. **绝对路径**：
   - 如果配置为 `table.list.file=/opt/inceptor-stat-collector/tables.txt`（绝对路径）
   - 文件放在指定的绝对路径下
   - 适合生产环境，便于统一管理

**配置示例：**

**示例1：使用相对路径（推荐）**
```properties
table.source.mode=file
table.list.file=tables.txt
```
- 文件放在程序运行目录下（与 `conf/` 目录同级）
- 适合单机部署
- **推荐位置**：项目根目录，与 `conf/config.properties` 一起管理

**示例2：使用相对路径（放在conf目录）**
```properties
table.source.mode=file
table.list.file=conf/tables.txt
```
- 文件放在 `conf/` 目录下，与配置文件一起管理
- 便于配置和表清单文件统一管理
- 示例目录结构：
  ```
  InceptorStatCollector/
  ├── bin/
  ├── libs/
  └── conf/
      ├── config.properties
      └── tables.txt          ← 文件放在conf目录下
  ```

**示例3：使用绝对路径**
```properties
table.source.mode=file
table.list.file=/opt/inceptor-stat-collector/tables.txt
```
- 文件位于指定绝对路径
- 适合生产环境，便于统一管理

**示例3：按环境区分**
```properties
# 生产环境
table.source.mode=file
table.list.file=/opt/inceptor-stat-collector/tables-prod.txt

# 测试环境
table.source.mode=file
table.list.file=/opt/inceptor-stat-collector/tables-test.txt
```
- 不同环境使用不同的表清单文件
- 便于管理和维护

**使用场景：**
1. **指定表统计**：只需要统计特定的表，而不是所有表
2. **临时统计**：需要临时统计某些表，不想修改 SQL 查询
3. **批量统计**：需要统计一批表，但不想每次都修改配置
4. **测试环境**：测试时只需要统计少量表

**注意事项：**
- 文件不存在时，程序会报错并退出
- 表名格式不正确时，会被跳过并记录警告
- 文件为空或所有表都被跳过时，程序会正常退出（没有表需要统计）
- 建议定期检查日志，确认所有表都被正确加载

**常见问题：**

**问题1：文件找不到**
- **错误**：`Table list file not found: tables.txt`
- **解决**：检查文件路径是否正确，文件是否存在

**问题2：表名格式不正确**
- **错误**：`Skipping invalid table name format at line X: table_name`
- **解决**：确保表名格式为 `database.table`，不能只有表名

**问题3：文件编码问题**
- **错误**：文件读取时出现乱码
- **解决**：确保文件使用 UTF-8 编码保存

## 工作原理

### 架构设计

程序采用**生产者-消费者模式**：

1. **生产者（AnalyzeWorker）**：
   - 从任务队列获取表名
   - 执行 `ANALYZE TABLE ... COMPUTE STATISTICS` 命令
   - 解析统计结果并放入结果队列

2. **消费者（ResultBatchWriter）**：
   - 从结果队列获取统计结果
   - 批量写入目标表
   - 使用事务保证数据一致性

### 执行流程

```
1. 根据 table.source.mode 配置选择表清单来源
   - sql 模式：从 system.tables_v 分页查询表列表
   - file 模式：从 table.list.file 文件读取表列表
   ↓
2. 生成 ANALYZE TABLE 命令并加入任务队列
   - 验证表名格式，防止SQL注入
   - 如果队列满，会等待工作线程处理
   ↓
3. 多个工作线程并发执行 ANALYZE 命令
   - 执行失败的表会自动记录到 failed.tables.file
   - 单个表失败不影响其他表的处理
   ↓
4. 解析 ANALYZE 返回的统计信息
   ↓
5. 根据 analyze.level 配置过滤结果
   - partition 模式：保留所有结果（表级别 + 分区级别）
   - table 模式：只保留表级别结果，过滤分区级别结果
   ↓
6. 批量写入目标表（批量大小可配置）
   ↓
7. 完成所有表的统计
   - 输出统计信息：总任务数、成功数、失败数等
```

### 统计级别处理机制

程序执行 `ANALYZE TABLE table_name COMPUTE STATISTICS;` 命令后，会返回以下格式的结果：

**表级别结果格式：**
```
table_name [numFiles=10, numRows=1000, totalSize=1024000, rawDataSize=1024000]
```

**分区级别结果格式：**
```
table_name{partition=value} [numFiles=5, numRows=500, totalSize=512000, rawDataSize=512000]
```

程序使用正则表达式解析这些结果，并根据 `analyze.level` 配置进行过滤：

- **partition 模式**：处理所有匹配的结果（包括表级别和分区级别）
- **table 模式**：只处理表级别的结果（过滤掉包含 `{partition}` 的结果）

**数据写入目标表：**
- `table_name`：表名
- `partition_name`：分区名称（表级别时为空字符串）
- `is_partition`：是否为分区表（0=否，1=是）
- 其他统计字段：`numFiles`、`numRows`、`totalSize`、`rawDataSize`

### 分页实现

由于 Inceptor/Hive 不支持 `OFFSET` 语法，程序使用 `ROW_NUMBER()` 窗口函数实现分页：

```sql
SELECT table_name FROM (
  SELECT table_name, ROW_NUMBER() OVER (ORDER BY table_name) as rn 
  FROM (基础查询SQL) base
) t WHERE rn > offset AND rn <= offset + pageSize
```

**分页逻辑说明：**
- 程序会逐页查询，直到某一页返回的记录数小于 `page.size`，表示已经是最后一页
- 判断逻辑基于数据库实际返回的记录数（`returned`），而不是成功加入队列的记录数（`valid`）
- 即使某些记录验证失败被跳过，也能正确判断是否还有更多数据
- 支持大规模表统计（2万+表），不会因为部分记录无效而提前停止

**分页日志输出：**
- 每页加载完成后会输出日志：`Loaded analysis tasks, page: X, returned: Y, valid: Z, total so far: W`
- `returned`：数据库实际返回的记录数（用于判断是否还有更多数据）
- `valid`：成功加入队列的记录数（验证通过的）
- `total`：累计加载的总任务数

## 目标表结构

程序会自动创建目标表（如果不存在），表结构如下：

```sql
CREATE TABLE IF NOT EXISTS default.inceptor_stats_result_table (
    id STRING COMMENT '唯一标识',
    data_time STRING COMMENT '插入时间',
    data_day DATE COMMENT '分区时间',
    table_name STRING COMMENT '表名',
    partition_name STRING COMMENT '分区名称',
    is_partition INT COMMENT '是否分区表',
    numFiles INT COMMENT '文件数',
    numRows INT COMMENT '行数',
    totalSize BIGINT COMMENT '总大小(字节)',
    rawDataSize BIGINT COMMENT '原始数据大小(字节)'
)
COMMENT '存放inceptor每张表的统计信息'
PARTITIONED BY RANGE (data_day)
INTERVAL (numtoyminterval('1','month'))
CLUSTERED BY (id) INTO 11 BUCKETS STORED AS ORC
TBLPROPERTIES('transactional'='true')
```

## 日志配置

### 日志框架

程序使用 **SLF4J + Log4j** 作为日志框架，所有日志消息均为英文，避免中文乱码问题。

### 日志配置文件

日志配置文件位于 `src/main/resources/log4j.properties`，默认配置：

- **控制台输出**：INFO 级别，UTF-8 编码
- **文件输出**：`D:/logs/inceptor-stat-collector.log`（可修改路径和级别）

### 日志级别说明

| 级别 | 说明 | 使用场景 |
|------|------|----------|
| INFO | 信息日志 | 程序启动、配置加载、任务完成、统计信息 |
| DEBUG | 调试日志 | 详细的执行过程、SQL语句、队列状态 |
| WARN | 警告日志 | 数据转换失败、非致命错误 |
| ERROR | 错误日志 | 异常情况、任务失败、数据库错误 |

### 详细日志信息

程序提供详细的日志输出，包括：

#### 1. 启动日志
- 程序启动时的配置信息（并发级别、批量大小、队列容量等）
- 连接池初始化信息（分析连接池和写入连接池的配置）
- 线程池启动信息

#### 2. 任务加载日志
- 任务加载开始和结束
- 每页加载的详细信息（页码、数量、累计总数）
- 总任务数统计

#### 3. 工作线程日志（AnalyzeWorker）
- 每个工作线程启动和结束时的统计信息
- 每个任务的处理详情（任务编号、SQL、记录数、解析数、耗时）
- 任务成功/失败统计
- 线程结束时的汇总统计（处理数、成功数、失败数、总耗时）

示例输出：
```
INFO  AnalyzeWorker-1 started
INFO  AnalyzeWorker-1 completed task #1, SQL: ANALYZE TABLE test COMPUTE STATISTICS;, records: 10, parsed: 10, elapsed: 100ms
INFO  AnalyzeWorker-1 finished, processed: 10, success: 10, failure: 0, total elapsed: 1000ms (1s)
```

#### 4. 批量写入日志（ResultBatchWriter）
- 写入线程启动和结束信息
- 队列状态定期输出（每10个批次输出一次）
- 每个批量写入的详细信息（批次号、记录数、有效/无效记录数、涉及的表数、耗时）
- 写入线程结束时的汇总统计（总批次、总记录、失败批次、失败记录、总耗时）

示例输出：
```
INFO  ResultBatchWriter thread started
INFO  Batch #1 write successful, records: 100 (valid: 100, invalid: 0), tables: 10, elapsed: 200ms
INFO  ResultBatchWriter thread finished, total batches: 10, total records: 1000, failed batches: 0, failed records: 0, total elapsed: 5000ms (5s)
```

#### 5. 程序结束日志
- 程序执行完成时的总耗时统计
- 所有任务的汇总信息

示例输出：
```
INFO  ========================================
INFO  InceptorStatCollector Completed
INFO  Total execution time: 5000ms (5 seconds)
INFO  ========================================
```

### 日志输出位置

- **控制台**：实时查看程序执行情况
- **日志文件**：持久化保存，便于后续分析和排查问题

### 日志格式

日志格式：`时间戳 - 耗时 - 级别 [线程名] 类名:行号 - 消息内容`

示例：
```
2025-12-16 14:41:41,733 - 0    INFO  [main] io.transwarp.InceptorStatCollector:118  - Target table created/verified successfully
```

### 自定义日志配置

如需修改日志配置，编辑 `src/main/resources/log4j.properties`：

- 修改日志级别：调整 `log4j.rootLogger` 的值
- 修改日志文件路径：修改 `log4j.appender.logfile.File` 的值
- 修改日志格式：调整 `ConversionPattern` 的值

## 配置文件加载顺序

程序按以下顺序加载配置文件：

1. **优先**：`conf/config.properties`（外部配置文件，推荐）
2. **备选**：jar包内部的 `config.properties`（打包时包含的默认配置）

如果外部配置文件存在，会优先使用外部配置；如果不存在，则使用jar包内部的默认配置。

启动时会显示加载的配置文件路径：
```
INFO  Loaded configuration from external file: /path/to/conf/config.properties
```
或
```
INFO  Loaded configuration from jar internal
```

## 常见问题

### 1. 配置文件找不到

**问题**：提示"未找到配置文件"

**解决方案**：
- 确保 `conf/config.properties` 文件存在
- 检查文件名是否正确（区分大小写）
- 检查文件权限（Linux/Mac需要读取权限）
- 如果使用外部配置文件，确保 `conf` 目录在项目根目录下

### 2. 连接数据库失败

- 检查数据库服务器是否可访问
- 验证用户名和密码是否正确
- 确认 JDBC URL 格式正确
- 检查网络连接和防火墙设置

### 2. 部分表分析失败

这是正常现象，可能原因：
- 表不存在或已被删除
- 权限不足
- 表格式不支持 ANALYZE 命令
- 表正在被其他操作锁定

程序会记录错误日志，但不会中断整体流程。

**失败表记录功能：**
- 程序会自动将执行失败的表名和日期记录到 `failed.tables.file` 指定的文件中
- 文件格式：`table_name,failed_date`（例如：`db.table,2025-12-16`）
- 可以使用该文件作为 `table.list.file` 的输入，通过 `table.source.mode=file` 模式重跑失败的表
- 默认文件位置：`conf/failed_tables.txt`

**重跑失败表的步骤：**
1. 查看 `conf/failed_tables.txt` 文件，确认失败的表
2. 提取表名（第一列），创建新的表清单文件（或直接使用该文件）
3. 配置 `table.source.mode=file` 和 `table.list.file=新文件路径`
4. 重新运行程序

**示例：**
```bash
# 1. 查看失败表记录文件
cat conf/failed_tables.txt
# 输出：
# db1.table1,2025-12-16
# db2.table2,2025-12-16

# 2. 提取表名创建新的表清单文件（只提取第一列）
cut -d',' -f1 conf/failed_tables.txt > conf/retry_tables.txt

# 3. 修改配置文件，设置 table.source.mode=file 和 table.list.file=conf/retry_tables.txt

# 4. 重新运行程序
```

### 3. 内存不足

如果遇到内存问题：
- 减小 `concurrency.level`（减少并发线程数）
- 减小 `batch.size`（减少批量写入大小）
- 减小 `result.queue.capacity`（减少队列容量）
- 减小 `page.size`（减少分页大小）

### 4. SQL 分页错误

如果遇到 SQL 解析错误：
- 确保配置的 `table.query.sql` 返回单列（表名）
- 检查 SQL 语法是否符合 Inceptor/Hive 规范
- 确保查询的表有访问权限

**分页加载不完整问题：**
- **问题**：表有2万多张，但只加载了部分表（如1985张）就停止了
- **原因**：可能是分页判断逻辑问题，或队列容量不足
- **解决方案**：
  1. 检查日志中的分页加载信息，确认实际加载了多少页
  2. 确保 `task.queue.capacity` 足够大（建议 >= 总表数 × 1.5）
  3. 检查是否有大量表名验证失败（查看日志中的警告信息）
  4. 如果使用SQL模式，确保 `page.size` 配置合理（建议1000-2000）
  5. 查看日志中的 `returned` 和 `valid` 数量，如果差异很大，说明有大量无效表名

**示例排查步骤：**
```bash
# 1. 查看日志中的分页加载信息
grep "Loaded analysis tasks" logs/inceptor-stat-collector.log

# 2. 查看是否有大量表名验证失败
grep "Skipping invalid table name" logs/inceptor-stat-collector.log | wc -l

# 3. 查看最终加载的任务数
grep "Task loading completed" logs/inceptor-stat-collector.log

# 4. 如果使用SQL模式，可以手动执行SQL验证总表数
# 在数据库中执行配置的 table.query.sql，查看返回的总记录数
```

### 5. 统计级别配置问题

**问题**：配置了 `analyze.level=table` 但仍然看到分区级别的数据

**解决方案**：
- 检查配置文件 `conf/config.properties` 中的 `analyze.level` 配置是否正确
- 确保配置值为 `table`（小写）
- 重启程序使配置生效
- 查看启动日志中的配置信息，确认当前使用的统计级别

**问题**：表级别模式下数据量没有减少

**可能原因**：
- 表都是非分区表，两种模式效果相同
- 配置未生效，检查配置文件路径和格式
- 查看启动日志确认配置是否正确加载

### 6. 超时问题

**问题**：程序提示"Analysis tasks did not complete within timeout period"

**解决方案**：
- 增加 `analysis.task.timeout.hours` 配置值
- 检查是否有表分析失败（查看错误日志）
- 检查数据库连接是否正常
- 考虑减少 `concurrency.level` 以降低数据库压力

**问题**：程序提示"Batch writer thread did not complete within timeout period"

**解决方案**：
- 增加 `batch.writer.timeout.minutes` 配置值
- 检查数据库写入性能
- 考虑减少 `batch.size` 以降低单次写入压力
- 检查目标表是否有锁或其他阻塞操作

**问题**：如何设置合适的超时时间？

**建议**：
- 先使用默认值运行一次，观察实际执行时间
- 如果经常超时，根据实际执行时间增加 20-50% 的缓冲时间
- 对于生产环境，建议设置较长的超时时间，避免因网络波动等原因导致任务中断
- 表数量 < 1000：建议 `analysis.task.timeout.hours=1-2`
- 表数量 1000-10000：建议 `analysis.task.timeout.hours=2-4`
- 表数量 > 10000：建议 `analysis.task.timeout.hours=4-8` 或更长

## 性能优化建议

1. **并发级别**：根据数据库服务器性能调整，建议从 5-10 开始，逐步增加
2. **批量大小**：根据网络延迟和数据库性能调整，建议 200-1000
3. **队列容量**：结果队列容量应至少为批量大小的 4 倍
4. **连接池**：程序已自动配置连接池，分析和写入使用独立连接池
5. **统计级别**：
   - 如果只需要表级别汇总，使用 `analyze.level=table` 可以减少数据量
   - 如果表都是分区表且需要详细统计，使用 `analyze.level=partition`（默认）
   - 表级别模式可以减少约 50-90% 的数据量（取决于分区数量）
   - 对于有大量分区的表，表级别模式可以显著提高处理速度
6. **超时配置**：
   - `analysis.task.timeout.hours`：根据表数量和复杂度调整
     - 表数量 < 1000：建议 1-2 小时
     - 表数量 1000-10000：建议 2-4 小时
     - 表数量 > 10000：建议 4-8 小时或更长
   - `batch.writer.timeout.minutes`：根据数据量调整
     - 数据量较小：建议 15-30 分钟
     - 数据量较大：建议 30-60 分钟
     - 数据量非常大：建议 60-120 分钟
   - 如果任务经常超时，可以适当增加超时时间
   - 建议先使用默认值运行，根据实际执行时间调整

## 依赖项

- **HikariCP 3.4.5**：数据库连接池
- **SLF4J 1.7.25**：日志框架
- **Log4j 1.2.17**：日志实现
- **Inceptor JDBC Driver**：Inceptor 数据库驱动（需自行提供）

## 许可证

本项目仅供内部使用。

## 更新日志

### v1.4
- ✅ 添加表清单文件读取功能（`table.source.mode=file`）
- ✅ 支持从文件读取表清单，便于指定特定表进行统计
- ✅ 添加失败表记录功能（`failed.tables.file`）
- ✅ 自动记录执行失败的表名和日期，方便后续重跑
- ✅ 修复分页加载逻辑，支持大规模表统计（2万+表）
- ✅ 改进分页判断逻辑，基于数据库实际返回记录数而非成功加入队列数
- ✅ 添加SQL注入防护，自动验证和清理表名
- ✅ 增强日志输出，显示分页加载的详细信息（returned/valid/total）
- ✅ 改进超时处理，超时后优雅关闭连接池，避免连接池关闭错误

### v1.3
- ✅ 添加超时配置功能
- ✅ `analysis.task.timeout.hours`：分析任务完成超时时间（小时），默认 2 小时
- ✅ `batch.writer.timeout.minutes`：批量写入线程完成超时时间（分钟），默认 30 分钟
- ✅ 超时配置支持默认值，未配置时使用默认值
- ✅ 日志输出显示实际配置的超时时间

### v1.2
- ✅ 添加统计级别配置功能（`analyze.level`）
- ✅ 支持表级别和分区级别两种统计模式
- ✅ 默认使用分区级别统计（`partition`）
- ✅ 表级别模式自动合并分区统计信息，减少数据量
- ✅ 表级别模式下结果表不包含 `partition_name` 和 `is_partition` 字段
- ✅ 配置项支持默认值，未配置时使用默认值

### v1.1
- ✅ 添加详细的日志输出功能
- ✅ 所有日志消息改为英文，解决中文乱码问题
- ✅ 添加程序启动、执行、结束的详细日志
- ✅ 添加工作线程和写入线程的统计日志
- ✅ 优化日志格式，包含时间戳、线程名、类名等信息
- ✅ 支持UTF-8编码，兼容Windows和Linux系统

### v1.0
- 初始版本
- 支持并发执行 ANALYZE 命令
- 支持批量写入统计结果
- 使用 ROW_NUMBER() 实现分页（兼容 Hive/Inceptor）

## 联系方式

如有问题或建议，请联系开发团队。

