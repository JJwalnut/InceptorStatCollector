#!/bin/bash
# InceptorStatCollector 停止脚本
# 用于优雅地停止正在运行的 InceptorStatCollector 程序

# 获取脚本所在目录，并返回到项目根目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

# 查找正在运行的 InceptorStatCollector 进程
# 通过类名查找
PID=$(ps aux | grep -i "io.transwarp.InceptorStatCollector" | grep -v grep | awk '{print $2}')

if [ -z "$PID" ]; then
    echo "未找到正在运行的 InceptorStatCollector 进程"
    exit 0
fi

echo "找到 InceptorStatCollector 进程，PID: $PID"
echo "正在优雅地停止程序..."

# 发送 SIGTERM 信号（优雅停止）
# SIGTERM 信号会触发 JVM ShutdownHook，程序会保存未完成的任务
kill -TERM "$PID" 2>/dev/null

if [ $? -ne 0 ]; then
    echo "错误: 无法发送停止信号到进程 $PID"
    exit 1
fi

# 等待进程退出（最多等待60秒）
TIMEOUT=60
ELAPSED=0
INTERVAL=2

while [ $ELAPSED -lt $TIMEOUT ]; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "程序已成功停止"
        exit 0
    fi
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
    echo "等待程序停止... (已等待 ${ELAPSED} 秒)"
done

# 如果超时仍未停止，检查进程是否还在运行
if ps -p "$PID" > /dev/null 2>&1; then
    echo "警告: 程序在 ${TIMEOUT} 秒内未能正常停止"
    echo "进程仍在运行，PID: $PID"
    echo ""
    echo "选项："
    echo "  1. 继续等待（程序可能正在保存未完成任务）"
    echo "  2. 强制终止（使用 kill -9，不推荐，可能导致未完成任务丢失）"
    echo ""
    read -p "是否强制终止？(y/N): " FORCE_KILL
    
    if [ "$FORCE_KILL" = "y" ] || [ "$FORCE_KILL" = "Y" ]; then
        echo "强制终止进程 $PID..."
        kill -9 "$PID" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo "进程已强制终止"
            echo "警告: 强制终止可能导致未完成任务未保存，请检查 pending.tables.file"
        else
            echo "错误: 无法强制终止进程"
            exit 1
        fi
    else
        echo "继续等待程序停止..."
        echo "提示: 可以使用 Ctrl+C 取消，然后手动检查进程状态"
        # 继续等待，直到进程退出
        while ps -p "$PID" > /dev/null 2>&1; do
            sleep 2
        done
        echo "程序已停止"
    fi
else
    echo "程序已成功停止"
fi

exit 0

