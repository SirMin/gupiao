"""
数据存储后端模块

支持存储格式：
- Parquet: 高性能列式存储
"""

from .base_storage import BaseStorage
from .parquet_storage import ParquetStorage

__all__ = ['BaseStorage', 'ParquetStorage']