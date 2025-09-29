"""
股票选择策略系统

基于多级回退策略和多因子评分模型的A股选股系统
"""

__version__ = "1.0.0"
__author__ = "Stock Selector Team"

# 导入核心组件
from .core.runner import StockSelectorRunner
from .core.strategy_engine import StrategyEngine
from .core.score_engine import ScoreEngine
from .core.preprocessor import StockDataPreprocessor

# 导入配置和工具
from .utils.config import ConfigManager
from .utils.logger import setup_logging, get_logger

__all__ = [
    'StockSelectorRunner',
    'StrategyEngine',
    'ScoreEngine',
    'StockDataPreprocessor',
    'ConfigManager',
    'setup_logging',
    'get_logger'
]