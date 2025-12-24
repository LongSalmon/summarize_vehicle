#!/usr/bin/env python3
"""
部署脚本：用于初始化和部署车辆管理系统
包含数据库初始化、表创建等功能
"""

import psycopg2
import logging
import sys
import os
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config_manager import ConfigManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deploy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def ensure_database_exists(host: str, port: int, user: str, password: str, dbname: str) -> bool:
    """
    确保数据库存在，如果不存在则创建
    
    :param host: 数据库主机地址
    :param port: 数据库端口号
    :param user: 数据库用户名
    :param password: 数据库密码
    :param dbname: 数据库名称
    :return: 是否成功确保数据库存在
    """
    conn = None
    cur = None
    
    try:
        # 连接到默认的postgres数据库
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname='postgres'
        )
        conn.autocommit = True  # 需要自动提交来创建数据库
        cur = conn.cursor()
        
        # 检查数据库是否存在
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()
        
        if not exists:
            logger.info(f"数据库 {dbname} 不存在，正在创建...")
            cur.execute(f"CREATE DATABASE {dbname}")
            logger.info(f"数据库 {dbname} 创建成功")
        else:
            logger.info(f"数据库 {dbname} 已存在")
        
        return True
    
    except psycopg2.Error as e:
        logger.error(f"创建数据库失败: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"检查/创建数据库时发生错误: {str(e)}")
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def init_postgres_db(host: str, port: int, user: str, password: str, dbname: str) -> Dict[str, Any]:
    """
    初始化PostgreSQL数据库，创建车辆管理系统所需的表结构
    
    :param host: 数据库主机地址
    :param port: 数据库端口号
    :param user: 数据库用户名
    :param password: 数据库密码
    :param dbname: 数据库名称
    :return: 包含操作结果的字典
    :raises Exception: 当初始化过程中出现错误时
    """
    conn = None
    cur = None
    result = {
        "success": False,
        "created_tables": [],
        "message": ""
    }
    
    try:
        # 建立数据库连接
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        conn.autocommit = False  # 开启事务
        cur = conn.cursor()
        
        logger.info("开始创建数据库表结构...")
        
        # 创建vehicle_info表（先创建，因为vehicle_trace会引用它）
        create_vehicle_info_sql = """
        CREATE TABLE IF NOT EXISTS vehicle_info (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100),
            phone_num VARCHAR(11) NOT NULL,
            plate VARCHAR(20) NOT NULL UNIQUE,
            vehicle_type VARCHAR(50),
            bonus float DEFAULT 1.0,
            last_record VARCHAR(20),
            last_record_time TIMESTAMP,
            mileage float DEFAULT 0,
            points float DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_vehicle_info_sql)
        result["created_tables"].append("vehicle_info")
        logger.info("✓ vehicle_info表创建成功")
        
        create_vehicle_record_sql = """
        CREATE TABLE IF NOT EXISTS vehicle_record (
            id SERIAL PRIMARY KEY,
            plate VARCHAR(20) NOT NULL, 
            mark VARCHAR(20) NOT NULL,
            pass_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_vehicle_record_sql)
        result["created_tables"].append("vehicle_record")
        logger.info("✓ vehicle_record表创建成功")

        
        # 添加索引以提高查询性能
        logger.info("创建索引以提高查询性能...")
        
        # 为vehicle_info表的plate字段创建索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vehicle_info_plate ON vehicle_info(plate);")
           
        # 提交事务
        conn.commit()
        
        result["success"] = True
        result["message"] = f"成功创建{len(result['created_tables'])}个表"
        logger.info(f"数据库初始化完成: {result['message']}")
        
        return result
        
    except psycopg2.Error as e:
        # 发生错误时回滚
        if conn:
            conn.rollback()
        error_msg = f"数据库操作失败: {str(e)}"
        result["message"] = error_msg
        logger.error(f"✗ {error_msg}")
        raise
    except Exception as e:
        # 发生其他错误
        if conn:
            conn.rollback()
        error_msg = f"初始化过程中发生错误: {str(e)}"
        result["message"] = error_msg
        logger.error(f"✗ {error_msg}")
        raise
    finally:
        # 确保资源正确关闭
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")


def main():
    """
    主函数：执行部署流程
    """
    logger.info("开始部署车辆管理系统...")
    
    try:
        # 加载配置文件
        config_manager = ConfigManager()
        db_config = config_manager.get("database")
        
        if not db_config:
            logger.error("无法从配置文件获取数据库配置")
            return 1
        
        logger.info(f"使用数据库配置: {db_config['host']}:{db_config['port']}@{db_config['dbname']}")
        
        # 确保数据库存在
        if not ensure_database_exists(**db_config):
            logger.error("数据库创建失败，部署终止")
            return 1
        
        # 初始化数据库表结构
        db_init_result = init_postgres_db(**db_config)
        if not db_init_result["success"]:
            logger.error(f"数据库表初始化失败: {db_init_result['message']}")
            return 1
        
        logger.info("车辆管理系统部署完成！")
        return 0
        
    except Exception as e:
        logger.error(f"部署失败: {str(e)}")
        logger.error("提示: 请确保PostgreSQL服务已启动，并且连接参数正确")
        return 1


if __name__ == "__main__":
    sys.exit(main())
