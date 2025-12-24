#!/bin/bash

# 车辆管理系统启动脚本

APP_DIR=$(dirname "$(readlink -f "$0")")
APP_NAME="vehicle_management"
LOG_FILE="$APP_DIR/vehicle_app.log"

# 检查是否已经在运行（使用更通用的模式）
if pgrep -f "python3.*app.py" > /dev/null; then
    echo "应用程序 $APP_NAME 已经在运行中"
    exit 1
fi

# 启动应用
echo "启动 $APP_NAME 应用..."
cd "$APP_DIR"
nohup python3 app.py > "$LOG_FILE" 2>&1 &

# 等待几秒钟让应用启动
sleep 2

# 检查是否启动成功
if pgrep -f "python3.*app.py" > /dev/null; then
    echo "$APP_NAME 应用启动成功！"
    echo "日志文件: $LOG_FILE"
    echo "可以使用以下命令查看日志: tail -f $LOG_FILE"
    echo "可以使用以下命令停止应用: kill $(pgrep -f "python3.*app.py")"
else
    echo "$APP_NAME 应用启动失败，请查看日志文件: $LOG_FILE"
    exit 1
fi
