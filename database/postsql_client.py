import psycopg2
import logging
from psycopg2 import sql
from typing import List, Dict, Any, Optional

# 获取或创建logger
logger = logging.getLogger(__name__)

# 自定义数据库错误异常
class DatabaseError(Exception):
    pass


class PostSQLClient:
    """
    PostgreSQL 数据交互接口封装
    提供增删改查（CRUD）基础操作
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """
        初始化连接参数
        """
        self.conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database
        }
        self.conn = None

    def connect(self):
        """
        建立数据库连接
        """
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            logger.info("PostgreSQL 连接成功")
        except psycopg2.Error as e:
            logger.error(f"连接失败: {e}")
            raise

    def close(self):
        """
        关闭数据库连接
        """
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL 连接已关闭")

    # ==================== 增（Create） ====================
    def insert(self, table: str, data: Dict[str, Any] = None, columns: List[str] = None, values: List[Any] = None) -> int:
        """
        插入单条记录，支持两种参数形式：
        1. 使用data字典：{column_name: value}
        2. 使用columns和values列表：columns=[column1, column2], values=[value1, value2]
        
        :param table: 表名
        :param data: 字段名->值的字典（与columns/values互斥）
        :param columns: 列名列表（与data互斥）
        :param values: 值列表（与data互斥）
        :return: 新插入记录的主键 id
        """
        # 确保连接已建立
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
            
        # 处理参数形式
        if data:
            columns_list = list(data.keys())
            values_list = list(data.values())
        elif columns and values:
            columns_list = columns
            values_list = values
        else:
            raise ValueError("必须提供data字典或columns/values列表")
        
        # 构建SQL查询
        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
            table=sql.Identifier(table),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns_list)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns_list))
        )
        
        # 尝试添加RETURNING id，但不依赖它（因为表可能没有id字段）
        # try:
        #     query = query + sql.SQL(" RETURNING id")
        # except Exception:
        #     pass
        
        with self.conn.cursor() as cur:
            try:
                cur.execute(query, values_list)
                # 尝试获取返回的id，但不依赖它
                try:
                    result = cur.fetchone()
                    if result:
                        return result[0]
                except:
                    pass
                return 1  # 返回成功插入的记录数
            except Exception as e:
                # 如果出错，检查是否需要回滚
                if not self.conn.autocommit:
                    self.conn.rollback()
                raise e

    def insert_many(self, table: str, data_list: List[Dict[str, Any]]) -> List[int]:
        """
        批量插入记录
        :param table: 表名
        :param data_list: 多条记录的字典列表
        :return: 新插入记录的主键 id 列表
        """
        if not data_list:
            return []
        columns = list(data_list[0].keys())
        values = [tuple(d[c] for c in columns) for d in data_list]
        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES {placeholders} RETURNING id").format(
            table=sql.Identifier(table),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        with self.conn.cursor() as cur:
            cur.executemany(query, values)
            new_ids = [row[0] for row in cur.fetchall()]
            return new_ids

    # ==================== 查（Read） ====================
    def select(self, table: str, columns: Optional[List[str]] = None,
               where: Optional[str] = None, params: Optional[tuple] = None,
               order_by: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        查询记录
        
        :param table: 表名
        :param columns: 需要查询的字段列表，None表示全部字段
        :param where: WHERE子句（不含WHERE关键字）
        :param params: WHERE子句对应的参数元组
        :param order_by: ORDER BY子句（不含ORDER BY关键字）
        :param limit: 限制返回记录数
        :return: 字典列表
        """
        # 确保连接已建立
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
            
        if columns:
            fields = sql.SQL(', ').join(map(sql.Identifier, columns))
        else:
            fields = sql.SQL('*')
        
        # 构建基础查询
        base_query = sql.SQL("SELECT {fields} FROM {table}").format(
            fields=fields,
            table=sql.Identifier(table)
        )
        
        # 构建完整查询
        query_parts = [base_query]
        
        if where:
            query_parts.append(sql.SQL(" WHERE ") + sql.SQL(where))
        
        if order_by:
            query_parts.append(sql.SQL(" ORDER BY ") + sql.SQL(order_by))
        
        if limit is not None:
            query_parts.append(sql.SQL(" LIMIT {limit}").format(limit=sql.Literal(limit)))
        
        query = sql.Composed(query_parts)
        
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]
            return [dict(zip(col_names, row)) for row in rows]

    def select_one(self, table: str, columns: Optional[List[str]] = None,
                   where: Optional[str] = None, params: Optional[tuple] = None, 
                   order_by: Optional[str] = None, limit: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        查询单条记录
        
        :param table: 表名
        :param columns: 需要查询的字段列表，None表示全部字段
        :param where: WHERE子句（不含WHERE关键字）
        :param params: WHERE子句对应的参数元组
        :param order_by: ORDER BY子句（不含ORDER BY关键字）
        :param limit: 限制返回记录数
        :return: 查询结果字典，如果没有找到记录则返回None
        """
        # 确保连接已建立
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
            
        results = self.select(table, columns, where, params, order_by, limit)
        return results[0] if results else None

    # ==================== 改（Update） ====================
    def update(self, table: str, data: Dict[str, Any], where: str, params: tuple) -> int:
        """
        更新记录
        :param table: 表名
        :param data: 需要更新的字段名->值字典
        :param where: WHERE 子句（不含 WHERE 关键字）
        :param params: WHERE 子句对应的参数元组
        :return: 受影响的行数
        """
        set_clauses = []
        values = []
        for k, v in data.items():
            if v is None:
                set_clauses.append(sql.SQL("{} = NULL").format(sql.Identifier(k)))
            else:
                set_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(k)))
                values.append(v)
        
        set_clause = sql.SQL(', ').join(set_clauses)
        query = sql.SQL("UPDATE {table} SET {set_clause} WHERE {where}").format(
            table=sql.Identifier(table),
            set_clause=set_clause,
            where=sql.SQL(where)
        )
        
        values += list(params)
        with self.conn.cursor() as cur:
            cur.execute(query, values)
            return cur.rowcount

    # ==================== 删（Delete） ====================
    def delete(self, table: str, where: str, params: tuple) -> int:
        """
        删除记录
        :param table: 表名
        :param where: WHERE 子句（不含 WHERE 关键字）
        :param params: WHERE 子句对应的参数元组
        :return: 受影响的行数
        """
        query = sql.SQL("DELETE FROM {table} WHERE {where}").format(
            table=sql.Identifier(table),
            where=sql.SQL(where)
        )
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def execute(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        直接执行SQL查询
        
        :param query: SQL查询语句
        :param params: 查询参数元组
        :return: 查询结果字典列表
        """
        # 确保连接已建立
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
            
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            # 如果是SELECT查询，返回结果
            if cur.description:
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
                return [dict(zip(col_names, row)) for row in rows]
            # 对于非SELECT查询，返回空列表
            return []
    
    # ==================== 事务控制（可选） ====================
    def begin(self):
        """
        手动开启事务
        """
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
        self.conn.autocommit = False

    def commit(self):
        """
        提交事务
        """
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
        self.conn.commit()

    def rollback(self):
        """
        回滚事务
        """
        if not self.conn:
            raise RuntimeError("数据库连接未建立，请先调用connect()方法")
        self.conn.rollback()

    def copy_from(self, file_object, table, sep=',', columns=None):
        """
        使用copy_from方法从文件对象导入数据到数据库表
        
        :param file_object: 文件对象
        :param table: 目标表名
        :param sep: 分隔符，默认为逗号
        :param columns: 列名列表
        :param header: 是否包含表头，默认为False
        """
        try:
            with self.conn.cursor() as cur:
                # if header:
                    # 跳过表头行
                    # file_object.readline()
                cur.copy_from(file_object, table, sep=sep, columns=columns)
        except Exception as e:
            raise DatabaseError(f"copy_from操作失败: {str(e)}")