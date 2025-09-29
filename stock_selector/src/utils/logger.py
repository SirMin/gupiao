"""
日志工具模块 - 配置和管理系统日志
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any
import sys


class LoggerManager:
    """日志管理器

    负责配置和管理系统日志，支持文件日志和控制台日志
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化日志管理器

        Args:
            config: 日志配置字典
        """
        self.config = config or self._get_default_config()
        self._setup_logging()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认日志配置"""
        return {
            'level': 'INFO',
            'file_path': './logs/stock_selector.log',
            'max_file_size': '10MB',
            'backup_count': 5,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': None,  # 日期格式，None表示使用默认
            'console_output': True,
            'console_format': None,  # 控制台独立格式，None表示使用format
            'file_format': None     # 文件独立格式，None表示使用format
        }

    def _parse_file_size(self, size_str: str) -> int:
        """解析文件大小字符串"""
        size_str = size_str.upper()
        multipliers = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 * 1024,
            'GB': 1024 * 1024 * 1024
        }

        for suffix, multiplier in multipliers.items():
            if size_str.endswith(suffix):
                return int(float(size_str[:-len(suffix)]) * multiplier)

        # 默认认为是字节
        return int(size_str)

    def _setup_logging(self):
        """设置日志配置"""
        # 获取根日志器
        root_logger = logging.getLogger()

        # 清除现有的处理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # 设置日志级别
        level = getattr(logging, self.config.get('level', 'INFO').upper())
        root_logger.setLevel(level)

        # 创建格式器
        date_format = self.config.get('date_format')
        base_format = self.config.get('format')

        # 控制台格式器
        console_format = self.config.get('console_format') or base_format
        console_formatter = logging.Formatter(console_format, datefmt=date_format)

        # 文件格式器
        file_format = self.config.get('file_format') or base_format
        file_formatter = logging.Formatter(file_format, datefmt=date_format)

        # 设置控制台处理器
        if self.config.get('console_output', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)

        # 设置文件处理器
        file_path = self.config.get('file_path')
        if file_path:
            try:
                # 确保日志目录存在
                log_dir = os.path.dirname(file_path)
                if log_dir:
                    os.makedirs(log_dir, exist_ok=True)

                # 解析文件大小
                max_size = self._parse_file_size(self.config.get('max_file_size', '10MB'))
                backup_count = self.config.get('backup_count', 5)

                # 创建旋转文件处理器
                file_handler = RotatingFileHandler(
                    filename=file_path,
                    maxBytes=max_size,
                    backupCount=backup_count,
                    encoding='utf-8'
                )
                file_handler.setLevel(level)
                file_handler.setFormatter(file_formatter)
                root_logger.addHandler(file_handler)

                print(f"日志文件配置成功: {file_path}")

            except Exception as e:
                print(f"配置文件日志失败: {str(e)}")

    def get_logger(self, name: str) -> logging.Logger:
        """
        获取指定名称的日志器

        Args:
            name: 日志器名称

        Returns:
            日志器实例
        """
        return logging.getLogger(name)

    def set_level(self, level: str):
        """
        设置日志级别

        Args:
            level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        log_level = getattr(logging, level.upper())
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        # 更新所有处理器的级别
        for handler in root_logger.handlers:
            handler.setLevel(log_level)

        self.config['level'] = level.upper()
        print(f"日志级别已设置为: {level.upper()}")

    def add_file_handler(self, file_path: str, level: str = 'INFO'):
        """
        添加额外的文件处理器

        Args:
            file_path: 文件路径
            level: 日志级别
        """
        try:
            # 确保目录存在
            log_dir = os.path.dirname(file_path)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)

            # 创建处理器
            handler = logging.FileHandler(file_path, encoding='utf-8')
            handler.setLevel(getattr(logging, level.upper()))

            # 设置格式
            formatter = logging.Formatter(self.config.get('format'))
            handler.setFormatter(formatter)

            # 添加到根日志器
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)

            print(f"添加文件处理器成功: {file_path}")

        except Exception as e:
            print(f"添加文件处理器失败: {str(e)}")

    def remove_console_output(self):
        """移除控制台输出"""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                root_logger.removeHandler(handler)
        self.config['console_output'] = False

    def enable_console_output(self):
        """启用控制台输出"""
        if not self.config.get('console_output', False):
            level = getattr(logging, self.config.get('level', 'INFO').upper())
            formatter = logging.Formatter(self.config.get('format'))

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)

            root_logger = logging.getLogger()
            root_logger.addHandler(console_handler)

            self.config['console_output'] = True

    @staticmethod
    def get_predefined_formats() -> Dict[str, Dict[str, str]]:
        """获取预定义的日志格式"""
        return {
            'simple': {
                'format': '%(levelname)s: %(message)s',
                'description': '简单格式 - 只显示级别和消息'
            },
            'basic': {
                'format': '%(asctime)s - %(levelname)s - %(message)s',
                'description': '基础格式 - 时间、级别、消息'
            },
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'description': '标准格式 - 时间、模块名、级别、消息'
            },
            'detailed': {
                'format': '%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s',
                'description': '详细格式 - 包含行号'
            },
            'debug': {
                'format': '%(asctime)s | %(process)d:%(thread)d | %(name)s:%(funcName)s:%(lineno)d | %(levelname)s | %(message)s',
                'description': '调试格式 - 包含进程、线程、函数信息'
            },
            'production': {
                'format': '%(asctime)s [%(levelname)-8s] %(name)-20s: %(message)s',
                'date_format': '%Y-%m-%d %H:%M:%S',
                'description': '生产格式 - 对齐美化的格式'
            },
            'json_like': {
                'format': '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
                'description': 'JSON风格格式 - 便于日志分析'
            }
        }

    def set_predefined_format(self, format_name: str):
        """
        设置预定义格式

        Args:
            format_name: 格式名称 (simple, basic, standard, detailed, debug, production, json_like)
        """
        formats = self.get_predefined_formats()
        if format_name not in formats:
            print(f"未知格式: {format_name}")
            print(f"可用格式: {list(formats.keys())}")
            return

        format_config = formats[format_name]
        self.config['format'] = format_config['format']

        if 'date_format' in format_config:
            self.config['date_format'] = format_config['date_format']

        # 重新设置日志
        self._setup_logging()
        print(f"日志格式已更新为: {format_name} - {format_config['description']}")

    def get_log_status(self) -> Dict[str, Any]:
        """
        获取日志系统状态

        Returns:
            日志状态信息
        """
        root_logger = logging.getLogger()

        handler_info = []
        for handler in root_logger.handlers:
            handler_type = type(handler).__name__
            if isinstance(handler, logging.FileHandler):
                handler_info.append({
                    'type': handler_type,
                    'file': handler.baseFilename,
                    'level': handler.level
                })
            else:
                handler_info.append({
                    'type': handler_type,
                    'level': handler.level
                })

        return {
            'level': logging.getLevelName(root_logger.level),
            'handlers': handler_info,
            'config': self.config
        }


# 全局日志管理器实例
_logger_manager = None


def setup_logging(config: Optional[Dict[str, Any]] = None) -> LoggerManager:
    """
    设置全局日志配置

    Args:
        config: 日志配置

    Returns:
        日志管理器实例
    """
    global _logger_manager
    _logger_manager = LoggerManager(config)
    return _logger_manager


def get_logger(name: str) -> logging.Logger:
    """
    获取日志器

    Args:
        name: 日志器名称

    Returns:
        日志器实例
    """
    global _logger_manager
    if _logger_manager is None:
        _logger_manager = LoggerManager()

    return _logger_manager.get_logger(name)


def set_log_level(level: str):
    """
    设置全局日志级别

    Args:
        level: 日志级别
    """
    global _logger_manager
    if _logger_manager is None:
        _logger_manager = LoggerManager()

    _logger_manager.set_level(level)


# 便捷函数
def debug(msg: str, logger_name: str = 'stock_selector'):
    """记录DEBUG级别日志"""
    get_logger(logger_name).debug(msg)


def info(msg: str, logger_name: str = 'stock_selector'):
    """记录INFO级别日志"""
    get_logger(logger_name).info(msg)


def warning(msg: str, logger_name: str = 'stock_selector'):
    """记录WARNING级别日志"""
    get_logger(logger_name).warning(msg)


def error(msg: str, logger_name: str = 'stock_selector'):
    """记录ERROR级别日志"""
    get_logger(logger_name).error(msg)


def critical(msg: str, logger_name: str = 'stock_selector'):
    """记录CRITICAL级别日志"""
    get_logger(logger_name).critical(msg)