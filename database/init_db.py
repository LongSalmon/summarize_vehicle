import psycopg2
import logging
from psycopg2 import sql
from typing import Dict, Any

# 获取或创建logger
logger = logging.getLogger(__name__)

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

if __name__ == "__main__":
    # 示例调用
    try:
        # 数据库连接参数
        config = {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "P@ssw0rd",
            "dbname": "vehicle_db"
        }
        logger.debug(f"使用配置: {config['host']}:{config['port']}@{config['dbname']}")
        
        # 初始化数据库
        result = init_postgres_db(**config)
        
        # 输出结果
        if result["success"]:
            logger.debug("数据库初始化成功!")
            logger.debug(f"创建的表: {', '.join(result['created_tables'])}")
        else:
            logger.error(f"数据库初始化失败: {result['message']}")
            
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        logger.error("提示: 请确保PostgreSQL服务已启动，并且连接参数正确")
        logger.error("请修改代码中的数据库连接参数为您的实际配置")
