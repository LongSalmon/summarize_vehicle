#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
车辆数据管理系统 - FLASK应用主文件

此应用提供Web界面用于管理车辆数据，包括：
- 数据库初始化
- 车辆信息导入
- 车辆轨迹导入
- 车辆最新状态查询
"""

import os
import sys
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from werkzeug.utils import secure_filename
from typing import Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from summarize.summarize import VehicleDataProcessor
from config.config_manager import config_manager

# 创建FLASK应用实例
app = Flask(__name__)

# 从配置文件加载应用配置
app.config['SECRET_KEY'] = config_manager.get('app.secret_key')
app.config['UPLOAD_FOLDER'] = config_manager.get('app.upload_folder', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads'))
app.config['ALLOWED_EXTENSIONS'] = set(config_manager.get('app.allowed_extensions', ['csv']))
app.config['MAX_CONTENT_LENGTH'] = config_manager.get('app.max_content_length', 16 * 1024 * 1024)  # 默认16MB

# 确保上传目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 全局数据库配置
DB_CONFIG = config_manager.get('database', {})

# 全局车辆数据处理器实例
VEHICLE_PROCESSOR: Optional[VehicleDataProcessor] = None

def allowed_file(filename: str) -> bool:
    """
    检查文件是否为允许的类型
    
    :param filename: 文件名
    :return: 是否允许上传
    """
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def get_vehicle_processor() -> VehicleDataProcessor:
    """
    获取或创建车辆数据处理器实例
    
    :return: VehicleDataProcessor实例
    """
    global VEHICLE_PROCESSOR
    if VEHICLE_PROCESSOR is None:
        VEHICLE_PROCESSOR = VehicleDataProcessor(**DB_CONFIG)
    return VEHICLE_PROCESSOR

@app.teardown_appcontext
def close_database_connection(exception: Optional[Exception] = None) -> None:
    """
    应用上下文结束时关闭数据库连接
    
    :param exception: 异常信息（如果有）
    """
    global VEHICLE_PROCESSOR
    if VEHICLE_PROCESSOR is not None:
        VEHICLE_PROCESSOR.close()
        VEHICLE_PROCESSOR = None

@app.route('/')
def index() -> str:
    """
    首页路由
    
    :return: 首页HTML
    """
    return render_template('index.html')



@app.route('/import-vehicle-info', methods=['POST'])
def import_vehicle_info():
    """
    车辆信息导入路由
    
    处理车辆信息CSV文件上传和导入
    
    :return: JSON响应，包含操作结果
    """
    try:
        # 检查是否有文件上传
        if 'vehicle_info_file' not in request.files:
            return jsonify(success=False, message='请选择要上传的文件！')
        
        file = request.files['vehicle_info_file']
        
        # 检查文件名是否为空
        if file.filename == '':
            return jsonify(success=False, message='请选择要上传的文件！')
        
        # 检查文件类型
        if not (file and allowed_file(file.filename)):
            return jsonify(success=False, message='只支持CSV文件格式！')
        
        # 保存文件到临时目录
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # 获取车辆数据处理器实例
            processor = get_vehicle_processor()
            
            # 导入车辆信息
            imported_count = processor.import_vehicle_info_from_csv(file_path)
            
            # 导入成功，记录日志
            app.logger.info(f'车辆信息导入成功: {imported_count} 条记录')
            
            return jsonify(success=True, message=f'车辆信息导入成功！共导入 {imported_count} 条记录。')
            
        except Exception as e:
            # 导入失败
            app.logger.error(f'车辆信息导入错误: {str(e)}')
            return jsonify(success=False, message=f'车辆信息导入失败: {str(e)}')
        finally:
            # 清理临时文件
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        app.logger.error(f'处理车辆信息文件时出错: {str(e)}')
        return jsonify(success=False, message=f'处理文件时出错: {str(e)}')

@app.route('/import-vehicle-trace', methods=['POST'])
def import_vehicle_trace():
    """
    车辆轨迹导入路由
    
    处理车辆轨迹CSV文件上传和导入
    
    :return: JSON响应，包含操作结果
    """
    try:
        # 检查是否有文件上传
        if 'vehicle_trace_file' not in request.files:
            return jsonify(success=False, message='请选择要上传的文件！')
        
        file = request.files['vehicle_trace_file']
        
        # 检查文件名是否为空
        if file.filename == '':
            return jsonify(success=False, message='请选择要上传的文件！')
        
        # 检查文件类型
        if not (file and allowed_file(file.filename)):
            return jsonify(success=False, message='只支持CSV文件格式！')
        
        # 保存文件到临时目录
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        try:
            # 获取车辆数据处理器实例
            processor = get_vehicle_processor()
            
            # 导入车辆轨迹
            imported_count = processor.import_vehicle_trace_from_csv(file_path)
            
            # 导入成功，记录日志
            app.logger.info(f'车辆轨迹导入成功: {imported_count} 条记录')
            
            return jsonify(success=True, message=f'车辆轨迹导入成功！共导入 {imported_count} 条记录。')
            
        except Exception as e:
            # 导入失败
            app.logger.error(f'车辆轨迹导入错误: {str(e)}')
            return jsonify(success=False, message=f'车辆轨迹导入失败: {str(e)}')
        finally:
            # 清理临时文件
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        app.logger.error(f'处理车辆轨迹文件时出错: {str(e)}')
        return jsonify(success=False, message=f'处理文件时出错: {str(e)}')

@app.route('/query-vehicle', methods=['POST'])
def query_vehicle():
    """
    车辆最新状态查询路由
    
    处理车辆状态查询请求，根据车辆ID查询车辆最新信息和轨迹
    
    :return: JSON响应，包含查询结果
    """
    try:
        # 获取表单数据
        vehicle_id = request.form.get('vehicle_id', '').strip()
        
        if not vehicle_id:
            return jsonify(success=False, message='请输入车辆ID！')
        
        # 获取车辆数据处理器实例
        processor = get_vehicle_processor()
        
        # 查询车辆状态
        vehicles = processor.query_vehicles(vehicle_id)
        
        # 返回结果到模板
        app.logger.info(f'车辆状态查询成功: 找到 {len(vehicles)} 条记录')
        
        # 如果没有查询到结果
        if not vehicles:
            return jsonify(success=True, message='未找到车辆信息，请检查车牌是否正确！', vehicles=[])
        
        # 返回JSON响应，包含查询结果
        return jsonify(success=True, message=f'查询成功，找到 {len(vehicles)} 条记录！', vehicles=vehicles)
        
    except Exception as e:
        app.logger.error(f'车辆状态查询错误: {str(e)}')
        return jsonify(success=False, message=f'查询出错: {str(e)}', vehicles=[])

@app.route('/undo-import', methods=['POST'])
def undo_import():
    """
    撤销导入路由
    
    清空过滤表中的所有数据，撤销之前的导入操作
    
    :return: 重定向到首页，显示操作结果消息
    """
    try:
        # 获取车辆数据处理器实例
        processor = get_vehicle_processor()
        
        # 清空过滤表
        with processor.db_connection():
            processor.postgresql_client.execute("TRUNCATE TABLE filtered_trace_data")
        
        # 操作成功，显示结果
        app.logger.info('撤销导入操作成功')
        return jsonify(success=True, message='已撤销所有导入操作，过滤表已清空！')
        
    except Exception as e:
        # 操作失败
        app.logger.error(f'撤销导入操作错误: {str(e)}')
        return jsonify(success=False, message=f'撤销导入操作失败: {str(e)}')

@app.route('/confirm-execution', methods=['POST'])
def confirm_execution():
    """
    确认执行路由
    
    处理TMP1表中的数据，生成车辆记录并更新车辆信息
    
    :return: 重定向到首页，显示操作结果消息
    """
    try:
        # 获取车辆数据处理器实例
        processor = get_vehicle_processor()
        
        # 执行数据处理
        success = processor.process_vehicle_data()
        
        if success:
            # 操作成功，显示结果
            app.logger.info('数据处理执行成功')
            return jsonify(success=True, message='数据处理成功，已生成车辆记录并更新车辆信息！')
        else:
            app.logger.error('数据处理执行失败')
            return jsonify(success=False, message='数据处理失败，请检查表中的数据格式！')
        
    except Exception as e:
        # 操作失败
        app.logger.error(f'确认执行操作错误: {str(e)}')
        return jsonify(success=False, message=f'确认执行操作失败: {str(e)}')

# 确保uploads目录存在
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

if __name__ == '__main__':
    # 在生产环境中应设置debug=False
    app.run(host='0.0.0.0', port=5000, debug=False)
