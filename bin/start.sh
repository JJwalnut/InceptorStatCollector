#!/bin/bash
# InceptorStatCollector 启动脚本

# 获取脚本所在目录，并返回到项目根目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

# 检查配置文件是否存在
if [ ! -f "conf/config.properties" ]; then
    echo "错误: 配置文件 conf/config.properties 不存在！"
    echo "请将配置文件放在 conf/ 目录下。"
    exit 1
fi

# 检查libs目录是否存在
LIB_DIR="libs"
if [ ! -d "$LIB_DIR" ]; then
    echo "错误: libs目录不存在: $LIB_DIR"
    echo "请先运行 mvn package 打包项目。"
    exit 1
fi

# 检查主jar包是否存在
JAR_FILE="libs/InceptorStatCollector-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "错误: jar包不存在: $JAR_FILE"
    echo "请先运行 mvn package 打包项目。"
    exit 1
fi

# 构建classpath（包含libs目录下所有jar包，使用绝对路径）
CP=""
for jar in "$PROJECT_ROOT/$LIB_DIR"/*.jar; do
    if [ -f "$jar" ]; then
        if [ -z "$CP" ]; then
            CP="$jar"
        else
            CP="$CP:$jar"
        fi
    fi
done

if [ -z "$CP" ]; then
    echo "错误: libs目录下没有找到jar包"
    exit 1
fi

# 设置UTF-8编码环境变量，解决中文乱码问题
export LANG=zh_CN.UTF-8
export LC_ALL=zh_CN.UTF-8

# 运行程序
echo "启动 InceptorStatCollector..."
echo "配置文件: $(pwd)/conf/config.properties"
echo "Jar包目录: $(pwd)/libs"
echo ""
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -Duser.language=zh -Duser.country=CN -cp "$CP" io.transwarp.InceptorStatCollector

