"""
股票数据源模块

提供统一的股票数据访问接口抽象定义
"""

from .base import StockDataSource, StockDataResult

__all__ = [
    'StockDataSource',
    'StockDataResult'
]