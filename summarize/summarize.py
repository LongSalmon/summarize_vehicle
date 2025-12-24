from typing import List, Dict, Any, Optional
from collections import defaultdict
from decimal import Decimal
from database.postgresql_client import PostgreSQLClient
import csv
import datetime
import re
import logging
import os
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 导入配置管理器
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config_manager import config_manager

# 配置日志
log_config = config_manager.get('logging', {})
logging.basicConfig(
    level=getattr(logging, log_config.get('level', 'INFO')),
    format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    handlers=[
        logging.FileHandler(log_config.get('file', 'vehicle_data.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def mark_parse(mark: str) -> float:
    """
    解析标记号，返回具体距离数字
    
    :param mark: 标记号，格式为 "K0001+100"
    :return: 具体距离
    :raises ValueError: 当标记号格式错误时
    """
    match = re.match(r"^K(\d+)\+(\d{3})$", mark.strip())
    if not match:
        raise ValueError(f"标记号格式错误：{mark}（应为Kx+y格式，如K3+500）")
    km_part = int(match.group(1))
    meter_part = int(match.group(2))
    return km_part + meter_part / 1000.0

def mileage_diff(mark1: str, mark2: str) -> float:
        """
        计算两个定位点之间的里程差
        
        :param loc1: 第一个定位点（如 "K0001+000"）
        :param loc2: 第二个定位点（如 "K0100+000"）
        :return: 里程差（公里）
        :raises ValueError: 当任意定位点不在标准路径中时
        """
        index1 = mark_parse(mark1)
        index2 = mark_parse(mark2)
        return abs(index1 - index2) / 1000.0


class VehicleDataProcessor:
    """
    车辆数据处理类，负责车辆信息的导入、分析和查询
    """
    
    def __init__(self, host, port, user, password, dbname):
        """
        初始化车辆数据处理器
        
        :param host: 数据库主机地址
        :param port: 数据库端口
        :param user: 数据库用户名
        :param password: 数据库密码
        :param dbname: 数据库名称
        """
        # 创建PostgreSQL客户端实例
        self.postgresql_client = PostgreSQLClient(host, port, user, password, dbname)
        
        # 从配置文件读取业务参数
        business_config = config_manager.get('business', {})
        self.standard_path = business_config.get('standard_path', 
                                               [["K0001+000", "K0100+000", "K0200+000", "K0300+000"],
                                                ["K0001+300", "K0100+300", "K0100+300", "K0100+300"]])
        self.max_threads_multiplier = business_config.get('max_threads_multiplier', 4)
        self.continuous_threshold = business_config.get('continuous_threshold', 1.0)

    def path_index(self, mark: Optional[str]) -> int:
        """
        获取定位点在标准路径中的索引
        
        :param loc: 定位点（如 "K0001+000"）
        :return: 索引位置（0-3）
        :raises ValueError: 当定位点不在标准路径中时
        """
        if mark is None:
            raise ValueError("标记号不能为None")
            
        for loc,path in enumerate(self.standard_path[0]):
            if mark in path:
                return loc
        else:
            for loc,path in enumerate(self.standard_path[1]):
                if mark in path:
                    return loc+10000
        raise ValueError(f"标记号 {mark} 不在标准路径中")

    def is_continuous(self, mark1: str, mark2: str) -> bool:
        """
        判断两个定位点是否连续
        
        :param loc1: 第一个定位点（如 "K0001+000"）
        :param loc2: 第二个定位点（如 "K0100+000"）
        :return: 如果连续则返回True，否则返回False
        :raises ValueError: 当任意定位点不在标准路径中时
        """
        index1 = self.path_index(mark1)
        index2 = self.path_index(mark2)
        return (index1 - index2 == 1)
    
    def close(self):
        """
        关闭数据库连接
        """
        if hasattr(self, 'postgresql_client') and self.postgresql_client:
            self.postgresql_client.close()
    
    @contextmanager
    def db_connection(self):
        """
        数据库连接上下文管理器，确保连接正确管理
        """
        try:
            # 如果连接不存在，则建立连接
            if not self.postgresql_client.conn:
                self.postgresql_client.connect()
            # 开启事务
            self.postgresql_client.begin()
            yield
            # 提交事务
            self.postgresql_client.commit()
        except Exception as e:
            # 回滚事务
            self.postgresql_client.rollback()
            raise e

    def import_vehicle_info_from_csv(self, csv_file_path: str) -> int:
        """
        从CSV文件导入车辆信息数据
        
        :param csv_file_path: CSV文件路径
        :return: 导入的记录数
        """
        imported_count = 0
        try:
            with self.db_connection():
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # 跳过标题行
                    for idx, row in enumerate(reader, start=1):
                        try:
                            if len(row) != 4:
                                logger.warning(f"第{idx}行数据格式错误，跳过")
                                continue
                                
                            username, phone_num, plate, vehicle_type = row
                            
                            # 使用字典形式插入数据，符合PostSQLClient的API
                            data = {
                                "username": username,
                                "phone_num": phone_num,
                                "plate": plate,
                                "vehicle_type": vehicle_type
                            }
                            self.postgresql_client.insert(table="vehicle_info", data=data)
                            imported_count += 1
                        except Exception as e:
                                logger.error(f"导入第{idx}行数据失败: {str(e)}")
            return imported_count
        except Exception as e:
            raise IOError(f"导入车辆信息失败: {str(e)}")

    def import_vehicle_trace_from_csv(self, csv_file_path: str) -> int:
        """
        从CSV文件导入车辆轨迹数据到过滤表（使用数据库原生导入方式）
        
        :param csv_file_path: CSV文件路径
        :return: 导入到过滤表的记录数
        """
        try:
            with self.db_connection():
                # 使用封装的方法创建并导入数据
                self.postgresql_client.drop_table_if_exists("raw_table")
                self.postgresql_client.create_raw_table()
                
                # 使用COPY命令将CSV数据导入到原始表
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    f.readline()  # 跳过表头行
                    self.postgresql_client.copy_from(f, 'raw_table', sep=',', columns=['plate', 'pass_time', 'mark'])
                
                self.postgresql_client.drop_table_if_exists("filtered_table")
                self.postgresql_client.create_filtered_table()
                
                # 从原始表导入数据到过滤表（append模式），并过滤重复项
                self.postgresql_client.import_from_raw_to_filtered()
                
                # 获取导入的记录数
                result = self.postgresql_client.execute("SELECT COUNT(*) AS imported_count FROM raw_table")
                imported_count = result[0]['imported_count'] if result else 0
                
                logger.info(f"成功导入 {imported_count} 条记录到 filtered_table 表")
                # 删除原始表，以便下次导入
                self.postgresql_client.drop_table_if_exists("raw_table")
                
                return imported_count
        except Exception as e:
            raise IOError(f"导入车辆轨迹数据失败: {str(e)}")

    def process_vehicle_data(self) -> bool:
        """
        处理过滤表中的数据，生成暂存表，并将数据添加到vehicle_record表
        
        :return: 处理成功返回True，失败返回False
        """
        try:
            with self.db_connection():
                # 使用封装的方法创建并填充暂存表
                self.postgresql_client.drop_table_if_exists("staging_table")
                self.postgresql_client.create_and_populate_staging()
                
                # 从暂存表导入数据到vehicle_record表
                self.postgresql_client.import_from_staging_to_vehicle_record()
                
                # 处理暂存表数据，更新vehicle_info表
                self._update_vehicle_info_from_staging()
                
                # 处理完成后删除临时表filtered_table和staging_table
                self.postgresql_client.drop_table_if_exists("filtered_table")
                self.postgresql_client.drop_table_if_exists("staging_table")
                
                return True
        except Exception as e:
            logger.error(f"处理车辆数据失败: {str(e)}")
            return False

    def _process_single_plate(self, plate: str, records: List[Dict[str, Any]]) -> bool:
        """
        处理单个车牌的记录，更新vehicle_info表
        
        :param plate: 车牌号
        :param records: 该车牌的所有记录
        :return: 处理成功返回True，失败返回False
        """
        try:
            # 为每个线程创建独立的数据库连接
            thread_local = threading.local()
            if not hasattr(thread_local, 'db_client'):
                thread_local.db_client = PostSQLClient(
                    self.postgresql_client.conn_params["host"], 
                    self.postgresql_client.conn_params["port"], 
                    self.postgresql_client.conn_params["user"], 
                    self.postgresql_client.conn_params["password"], 
                    self.postgresql_client.conn_params["database"]
                )
                thread_local.db_client.connect()
            
            db_client = thread_local.db_client
            
            logger.info(f"处理车牌: {plate}")
            # 查询vehicle_info表中的当前记录
            vehicle_info = db_client.select(
                "vehicle_info",
                ["last_record", "last_record_time", "mileage", "bonus"],
                where="plate = %s",
                params=(plate,)
            )

            if not vehicle_info:
                # 如果车辆信息不存在，跳过处理
                logger.warning(f"车牌 {plate} 的车辆信息不存在，跳过处理")
                return True

            last_record = vehicle_info[0]["last_record"]
            last_record_time = vehicle_info[0]["last_record_time"]
            mileage = vehicle_info[0]["mileage"]
            bonus = vehicle_info[0]["bonus"]

            logger.debug(f"车牌 {plate} 的初始信息 - last_record: {last_record}, last_record_time: {last_record_time}, mileage: {mileage}")

            for i, record in enumerate(records):
                mark = record['mark']
                pass_time = record['pass_time']
                
                if last_record_time is not None and pass_time < last_record_time:
                    # 如果当前记录的时间早于上一条记录的时间，跳过处理
                    logger.debug(f"记录 {i+1}/{len(records)} - mark: {mark}, pass_time: {pass_time} 早于上一条记录的时间 {last_record_time}，跳过处理")
                    continue

                logger.debug(f"处理记录 {i+1}/{len(records)} - mark: {mark}, pass_time: {pass_time}, last_record: {last_record}")
                
                # 只有当mark和last_record都不是None时才计算里程差
                if mark is not None and last_record is not None:
                    logger.debug(f"mark和last_record都不为None，调用is_continuous方法")
                    if self.is_continuous(mark, last_record):
                        logger.debug(f"记录连续，计算里程差")
                        # 将float类型的里程差转换为Decimal类型，与数据库中的mileage字段类型匹配
                        diff = mileage_diff(mark, last_record)
                        mileage += diff
                
                last_record = mark
                last_record_time = pass_time

            points = mileage * bonus
            db_client.update(
                "vehicle_info",
                {
                    "last_record": last_record,
                    "last_record_time": last_record_time,
                    "mileage": mileage,
                    "points": points
                },
                where="plate = %s",
                params=(plate,)
            )
            
            return True
        except Exception as e:
            import traceback
            logger.error(f"处理车牌 {plate} 失败: {str(e)}")
            logger.error(f"异常类型: {type(e).__name__}")
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            return False
        finally:
            # 线程结束时关闭数据库连接
            thread_local = threading.local()
            if hasattr(thread_local, 'db_client'):
                thread_local.db_client.close()
                delattr(thread_local, 'db_client')

    def _update_vehicle_info_from_staging(self) -> None:
        """
        根据暂存表中的数据更新vehicle_info表的last_record、mileage和points字段
        使用线程池并行处理多个车牌
        
        :return: None
        """
        try:
            # 查询暂存表中的所有记录，按车牌和时间排序
            staging_data = self.postgresql_client.select(
                "staging_table",
                ["plate", "mark", "pass_time"],
                order_by="plate, pass_time"
            )
            logger.debug(f"staging_data: {staging_data}")
            
            # 按车牌分组处理数据
            plate_groups = defaultdict(list)
            for record in staging_data:
                plate = record['plate']
                plate_groups[plate].append(record)
            
            # 使用线程池并行处理所有车牌
            logger.info(f"开始并行处理 {len(plate_groups)} 个车牌的数据")
            
            # 根据系统CPU核心数和配置的倍数设置线程池大小
            max_workers = min(len(plate_groups), os.cpu_count() * self.max_threads_multiplier)
            logger.info(f"使用 {max_workers} 个线程进行并行处理")
            
            success_count = 0
            failure_count = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_plate = {
                    executor.submit(self._process_single_plate, plate, records): plate
                    for plate, records in plate_groups.items()
                }
                
                # 处理任务结果
                for future in as_completed(future_to_plate):
                    plate = future_to_plate[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            failure_count += 1
                    except Exception as e:
                        import traceback
                        logger.error(f"处理车牌 {plate} 时发生异常: {str(e)}")
                        logger.error(f"异常类型: {type(e).__name__}")
                        logger.error(f"异常堆栈: {traceback.format_exc()}")
                        failure_count += 1
            
            logger.info(f"并行处理完成 - 成功: {success_count}, 失败: {failure_count}")

        except Exception as e:
            import traceback
            logger.error(f"更新vehicle_info表失败: {str(e)}")
            logger.error(f"异常类型: {type(e).__name__}")
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            raise

    def query_vehicles(self, plate: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        联结vehicle_trace与vehicle_info表，查询车辆最新状态
        
        :param plate: 车牌号（可选），不传则查询所有
        :return: 包含车辆综合信息的列表
        """
        try:
            with self.db_connection():
                # 构建完整的SQL查询

                return self.postgresql_client.select(
                    "vehicle_info",
                    ["plate", "username", "phone_num", "vehicle_type", "bonus", "points", "mileage", "last_record", "last_record_time"],
                    where="plate = %s" if plate else None,
                    params=(plate,) if plate else None
                )
        except Exception as e:
            logger.error(f"查询车辆信息失败: {str(e)}")
            return []