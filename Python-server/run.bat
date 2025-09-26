@echo off
setlocal enabledelayedexpansion

REM 获取当前脚本所在目录
set "SCRIPT_DIR=%~dp0"

REM 启动 prediction_server.py 9090
start cmd /k "cd /d %SCRIPT_DIR% && python prediction_server.py 9090"

REM 启动 prediction_server.py 9091
start cmd /k "cd /d %SCRIPT_DIR% && python prediction_server.py 9091"

echo 两个服务已启动 (9090 和 9091)。
endlocal
