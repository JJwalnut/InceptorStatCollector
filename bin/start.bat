@echo off
setlocal enabledelayedexpansion
REM InceptorStatCollector 启动脚本 (Windows)

REM 获取脚本所在目录，并返回到项目根目录
cd /d %~dp0
cd ..

REM 检查配置文件是否存在
if not exist "conf\config.properties" (
    echo 错误: 配置文件 conf\config.properties 不存在！
    echo 请将配置文件放在 conf\ 目录下。
    pause
    exit /b 1
)

REM 检查libs目录是否存在
if not exist "libs" (
    echo 错误: libs目录不存在
    echo 请先运行 mvn package 打包项目。
    pause
    exit /b 1
)

REM 检查主jar包是否存在
if not exist "libs\InceptorStatCollector-1.0-SNAPSHOT.jar" (
    echo 错误: jar包不存在: libs\InceptorStatCollector-1.0-SNAPSHOT.jar
    echo 请先运行 mvn package 打包项目。
    pause
    exit /b 1
)

REM 构建classpath（包含libs目录下所有jar包，使用绝对路径）
set CP=
for %%f in ("%CD%\libs\*.jar") do (
    if "!CP!"=="" (
        set CP=%%f
    ) else (
        set CP=!CP!;%%f
    )
)

if "!CP!"=="" (
    echo 错误: libs目录下没有找到jar包
    pause
    exit /b 1
)

REM 运行程序
echo 启动 InceptorStatCollector...
echo 配置文件: %CD%\conf\config.properties
echo Jar包目录: %CD%\libs
echo.

REM 设置UTF-8编码，解决中文乱码问题
chcp 65001 >nul 2>&1
java -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8 -cp "!CP!" io.transwarp.InceptorStatCollector

pause

