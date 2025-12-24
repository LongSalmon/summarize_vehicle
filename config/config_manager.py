import json
import os
import logging
from typing import Dict, Any

# 获取或创建logger
logger = logging.getLogger(__name__)

class ConfigManager:
    """
    配置管理类，负责加载和提供配置参数
    """
    
    _instance = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls, config_file_path: str = None):
        """
        单例模式，确保只有一个ConfigManager实例
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            if config_file_path:
                cls._instance.load_config(config_file_path)
        return cls._instance
    
    def load_config(self, config_file_path: str):
        """
        从指定的配置文件加载配置
        
        :param config_file_path: 配置文件路径
        :raises FileNotFoundError: 如果配置文件不存在
        :raises json.JSONDecodeError: 如果配置文件格式错误
        """
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"配置文件不存在: {config_file_path}")
        
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            logger.info(f"成功加载配置文件: {config_file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"配置文件格式错误: {str(e)}")
            raise
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置参数
        
        :param key: 配置键，支持点号分隔（如 "database.host"）
        :param default: 默认值，如果键不存在则返回此值
        :return: 配置值
        """
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_all(self) -> Dict[str, Any]:
        """
        获取所有配置
        
        :return: 完整的配置字典
        """
        return self._config

# 默认配置文件路径
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'config.json'
)

# 创建全局配置管理器实例
try:
    config_manager = ConfigManager(DEFAULT_CONFIG_PATH)
except FileNotFoundError:
    logger.warning(f"默认配置文件不存在: {DEFAULT_CONFIG_PATH}")
    config_manager = ConfigManager()
