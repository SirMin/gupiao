"""
缓存数据源模块

提供智能缓存功能的股票数据源实现，支持：
- 历史数据的增量缓存
- 元数据索引管理
- 范围查询优化
- 高性能Parquet存储
- DuckDB SQL查询引擎
- 多数据源故障转移
"""

from .cached_datasource import CachedDataSource
from .metadata_manager import MetadataManager
from .file_cache import FileCache
from .range_calculator import DateRange, RangeCalculator
from .datasource_manager import DataSourceManager
from .query_engine import DuckDBQueryEngine

__all__ = [
    'CachedDataSource',
    'MetadataManager',
    'FileCache',
    'DateRange',
    'RangeCalculator',
    'DataSourceManager',
    'DuckDBQueryEngine'
]