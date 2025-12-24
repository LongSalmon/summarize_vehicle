#!/bin/bash

# 车辆管理系统部署脚本
# 用于在新电脑上完整部署项目，包括解压项目文件、数据库安装、Python环境配置和依赖部署

set -e  # 出错时立即退出

# 日志颜色设置
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} 车辆管理系统部署脚本${NC}"
echo -e "${GREEN}========================================${NC}"

# 默认配置
DEFAULT_INSTALL_DIR="/opt/vehicle_management"
DEFAULT_PROJECT_FILE="vehicle_management.tar.gz"

# 检查操作系统类型
check_os() {
    echo -e "${YELLOW}检查操作系统类型...${NC}"
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        VERSION=$VERSION_ID
        echo -e "${GREEN}检测到操作系统: $OS $VERSION${NC}"
    else
        echo -e "${RED}无法检测操作系统类型${NC}"
        exit 1
    fi
}

# 解压项目文件
extract_project() {
    echo -e "${YELLOW}解压项目文件...${NC}"
    
    # 获取项目压缩文件和安装目录
    read -p "请输入项目压缩文件路径（默认：$DEFAULT_PROJECT_FILE）: " PROJECT_FILE
    PROJECT_FILE=${PROJECT_FILE:-$DEFAULT_PROJECT_FILE}
    
    read -p "请输入安装目录（默认：$DEFAULT_INSTALL_DIR）: " INSTALL_DIR
    INSTALL_DIR=${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}
    
    # 检查项目文件是否存在
    if [ ! -f "$PROJECT_FILE" ]; then
        echo -e "${RED}项目文件 $PROJECT_FILE 不存在${NC}"
        exit 1
    fi
    
    # 创建安装目录
    sudo mkdir -p "$INSTALL_DIR"
    
    # 解压项目文件
    if [[ "$PROJECT_FILE" == *.tar.gz || "$PROJECT_FILE" == *.tgz ]]; then
        echo -e "${YELLOW}解压tar.gz文件...${NC}"
        sudo tar -xzf "$PROJECT_FILE" -C "$INSTALL_DIR"
    elif [[ "$PROJECT_FILE" == *.zip ]]; then
        echo -e "${YELLOW}解压zip文件...${NC}"
        sudo unzip -o "$PROJECT_FILE" -d "$INSTALL_DIR"
    else
        echo -e "${RED}不支持的文件格式，仅支持tar.gz/tgz和zip格式${NC}"
        exit 1
    fi
    
    # 进入安装目录
    cd "$INSTALL_DIR"
    
    # 检查解压是否成功（是否包含关键文件）
    if [ ! -f "app.py" ]; then
        echo -e "${RED}项目文件解压失败或格式不正确，未找到app.py${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}项目文件解压完成，安装目录：$INSTALL_DIR${NC}"
}

# 安装PostgreSQL
export_postgresql() {
    echo -e "${YELLOW}安装PostgreSQL数据库...${NC}"
    
    if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
        # Ubuntu/Debian安装PostgreSQL
        sudo apt-get update
        sudo apt-get install -y postgresql postgresql-contrib libpq-dev
        sudo systemctl start postgresql
        sudo systemctl enable postgresql
    elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ]; then
        # CentOS/RHEL安装PostgreSQL
        sudo yum install -y postgresql-server postgresql-devel
        sudo postgresql-setup --initdb
        sudo systemctl start postgresql
        sudo systemctl enable postgresql
    else
        echo -e "${YELLOW}暂不支持的操作系统，跳过PostgreSQL自动安装${NC}"
        echo -e "${YELLOW}请手动安装PostgreSQL并确保服务运行${NC}"
        return 1
    fi
    
    echo -e "${GREEN}PostgreSQL安装完成${NC}"
    return 0
}

# 配置PostgreSQL用户和数据库
configure_postgresql() {
    echo -e "${YELLOW}配置PostgreSQL...${NC}"
    
    # 读取配置文件中的数据库参数
    if [ -f "config.json" ]; then
        HOST=$(cat config.json | grep -oP '"host":\s*"\K[^"]*')
        PORT=$(cat config.json | grep -oP '"port":\s*\K[0-9]+')
        USER=$(cat config.json | grep -oP '"user":\s*"\K[^"]*')
        PASSWORD=$(cat config.json | grep -oP '"password":\s*"\K[^"]*')
        DBNAME=$(cat config.json | grep -oP '"dbname":\s*"\K[^"]*')
    else
        # 使用默认配置
        HOST="localhost"
        PORT="5432"
        USER="postgres"
        PASSWORD="P@ssw0rd"
        DBNAME="vehicle_db"
    fi
    
    echo -e "${GREEN}使用数据库配置: ${HOST}:${PORT}@${DBNAME}${NC}"
    
    # 设置PostgreSQL用户密码
    if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
        sudo -u postgres psql -c "ALTER USER $USER WITH PASSWORD '$PASSWORD';"
    elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ]; then
        sudo -u postgres psql -c "ALTER USER $USER WITH PASSWORD '$PASSWORD';"
    else
        echo -e "${YELLOW}跳过PostgreSQL用户密码设置${NC}"
    fi
    
    echo -e "${GREEN}PostgreSQL配置完成${NC}"
}

# 检查Python环境
check_python() {
    echo -e "${YELLOW}检查Python环境...${NC}"
    
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version | grep -oP 'Python \K[0-9]+\.[0-9]+')
        echo -e "${GREEN}检测到Python: $PYTHON_VERSION${NC}"
        
        # 检查Python版本是否符合要求 (>=3.8)
        IFS='.' read -r major minor <<< "$PYTHON_VERSION"
        if (( major > 3 )) || (( major == 3 && minor >= 8 )); then
            echo -e "${GREEN}Python版本符合要求(>=3.8)${NC}"
            return 0
        else
            echo -e "${RED}Python版本过低，请安装Python 3.8或更高版本${NC}"
            return 1
        fi
    else
        echo -e "${RED}未检测到Python 3，请先安装Python 3.8或更高版本${NC}"
        return 1
    fi
}

# 安装Python依赖
install_dependencies() {
    echo -e "${YELLOW}安装Python依赖...${NC}"
    
    if [ -f "requirements.txt" ]; then
        pip3 install -r requirements.txt
        echo -e "${GREEN}依赖安装完成${NC}"
    else
        echo -e "${RED}未找到requirements.txt文件${NC}"
        return 1
    fi
}

# 初始化数据库
export_database() {
    echo -e "${YELLOW}初始化数据库...${NC}"
    
    # 使用Python脚本初始化数据库
    python3 deploy.py
    
    echo -e "${GREEN}数据库初始化完成${NC}"
}

# 设置文件权限
set_permissions() {
    echo -e "${YELLOW}设置文件权限...${NC}"
    
    # 创建必要的目录
    sudo mkdir -p uploads data
    sudo chmod -R 755 uploads data
    
    # 使脚本可执行
    sudo chmod +x deploy.sh
    
    echo -e "${GREEN}文件权限设置完成${NC}"
}

# 创建start脚本
create_start_script() {
    echo -e "${YELLOW}创建启动脚本...${NC}"
    
    # 创建start.sh脚本
    cat > start.sh << 'EOF'
#!/bin/bash

# 车辆管理系统启动脚本

APP_DIR=$(dirname "$(readlink -f "$0")")
APP_NAME="vehicle_management"
LOG_FILE="$APP_DIR/vehicle_app.log"

# 检查是否已经在运行
if pgrep -f "python3 $APP_DIR/app.py" > /dev/null; then
    echo "应用程序 $APP_NAME 已经在运行中"
    exit 1
fi

# 启动应用
echo "启动 $APP_NAME 应用..."
cd "$APP_DIR"
nohup python3 app.py > "$LOG_FILE" 2>&1 &

# 检查是否启动成功
if pgrep -f "python3 $APP_DIR/app.py" > /dev/null; then
    echo "$APP_NAME 应用启动成功！"
    echo "日志文件: $LOG_FILE"
    echo "可以使用以下命令查看日志: tail -f $LOG_FILE"
    echo "可以使用以下命令停止应用: kill $(pgrep -f "python3 $APP_DIR/app.py")"
else
    echo "$APP_NAME 应用启动失败，请查看日志文件: $LOG_FILE"
    exit 1
fi
EOF
    
    # 设置执行权限
    sudo chmod +x start.sh
    
    echo -e "${GREEN}启动脚本创建完成${NC}"
}

# 显示部署完成信息
show_completion() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN} 部署完成！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "${YELLOW}使用说明:${NC}"
    echo -e "  1. 启动应用: ${GREEN}./start.sh${NC}"
    echo -e "  2. 访问地址: ${GREEN}http://localhost:5000${NC}"
    echo -e "  3. 上传CSV文件进行车辆数据处理${NC}"
    echo -e "  4. 查看日志: ${GREEN}tail -f vehicle_app.log${NC}"
    echo -e "  5. 停止应用: ${GREEN}kill $(pgrep -f "python3 $PWD/app.py")${NC}"
    echo -e "${YELLOW}注意事项:${NC}"
    echo -e "  - 确保PostgreSQL服务正在运行${NC}"
    echo -e "  - 如需修改配置，请编辑 config.json 文件${NC}"
    echo -e "  - 应用程序将在后台运行(nohup)${NC}"
}

# 主函数
main() {
    check_os
    extract_project
    
    # 尝试安装PostgreSQL，失败则继续
    export_postgresql || true
    
    configure_postgresql
    
    if check_python; then
        install_dependencies
        export_database
        set_permissions
        create_start_script
        show_completion
    else
        echo -e "${RED}Python环境检查失败，部署终止${NC}"
        exit 1
    fi
}

# 执行主函数
main