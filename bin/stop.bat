@echo off
REM InceptorStatCollector 停止脚本 (Windows)
REM 用于优雅地停止正在运行的 InceptorStatCollector 程序

REM 获取脚本所在目录，并返回到项目根目录
cd /d "%~dp0\.."

REM 查找正在运行的 InceptorStatCollector 进程
REM 通过类名查找 Java 进程
for /f "tokens=2" %%i in ('tasklist /FI "IMAGENAME eq java.exe" /FO CSV ^| findstr /I "InceptorStatCollector"') do (
    set PID=%%i
    goto :found
)

echo 未找到正在运行的 InceptorStatCollector 进程
exit /b 0

:found
echo 找到 InceptorStatCollector 进程，PID: %PID%
echo 正在优雅地停止程序...

REM 发送停止信号（Windows 使用 taskkill）
taskkill /PID %PID% /T 2>nul

if errorlevel 1 (
    echo 错误: 无法停止进程 %PID%
    exit /b 1
)

echo 停止信号已发送，程序正在优雅关闭...
echo 提示: 程序会保存未完成的任务到 pending.tables.file
echo 等待程序退出...

REM 等待进程退出（最多等待60秒）
set TIMEOUT=60
set ELAPSED=0
set INTERVAL=2

:wait_loop
timeout /t %INTERVAL% /nobreak >nul 2>&1
tasklist /FI "PID eq %PID%" 2>nul | findstr /I "%PID%" >nul
if errorlevel 1 (
    echo 程序已成功停止
    exit /b 0
)

set /a ELAPSED+=%INTERVAL%
if %ELAPSED% geq %TIMEOUT% (
    echo 警告: 程序在 %TIMEOUT% 秒内未能正常停止
    echo 进程可能仍在运行，PID: %PID%
    echo.
    echo 选项：
    echo   1. 继续等待（程序可能正在保存未完成任务）
    echo   2. 强制终止（可能导致未完成任务丢失）
    echo.
    set /p FORCE_KILL=是否强制终止？(Y/N): 
    
    if /i "%FORCE_KILL%"=="Y" (
        echo 强制终止进程 %PID%...
        taskkill /PID %PID% /F 2>nul
        if errorlevel 1 (
            echo 错误: 无法强制终止进程
            exit /b 1
        )
        echo 进程已强制终止
        echo 警告: 强制终止可能导致未完成任务未保存，请检查 pending.tables.file
    ) else (
        echo 继续等待程序停止...
        echo 提示: 可以使用 Ctrl+C 取消，然后手动检查进程状态
        goto :wait_loop
    )
) else (
    echo 等待程序停止... (已等待 %ELAPSED% 秒)
    goto :wait_loop
)

exit /b 0

