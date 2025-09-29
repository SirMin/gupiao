# 智能缓存数据源架构设计

## 概述

智能缓存数据源（CachedDataSource）是一个专门为历史股票数据设计的缓存层，通过元数据索引和范围查询优化，实现高效的本地数据缓存和增量更新。

## 核心特性

- **智能范围查询**：自动计算缺失数据范围，最小化远程请求
- **元数据索引**：维护已缓存数据的完整索引，快速判断数据可用性
- **增量更新**：支持历史数据的渐进式缓存，避免重复下载
- **高效存储**：采用 Parquet 列式存储格式，优化查询性能
- **DuckDB查询引擎**：集成高性能分析型数据库，支持复杂SQL查询和聚合计算
- **多数据源支持**：支持多个远程数据源，自动故障转移和负载均衡
- **透明接口**：完全兼容 StockDataSource 接口，无需修改现有代码

## 架构设计

### 目录结构

```
gupiao/datasource/cache/
├── __init__.py                 # 模块入口
├── README.md                   # 架构文档（本文件）
├── cached_datasource.py        # 主缓存数据源类
├── metadata_manager.py         # 元数据管理器
├── file_cache.py              # 文件缓存管理
├── range_calculator.py        # 日期范围计算工具
├── query_engine.py            # DuckDB查询引擎封装
├── datasource_manager.py      # 多数据源管理器
└── storage/                   # 存储后端实现
    ├── __init__.py
    ├── base_storage.py        # 存储后端抽象基类
    └── parquet_storage.py     # Parquet 格式存储
```

### 核心组件

#### 1. CachedDataSource (cached_datasource.py)

**职责**：缓存数据源的主入口，协调各个组件工作

**核心方法**：
```python
class CachedDataSource(StockDataSource):
    def __init__(self, remote_datasources: Union[StockDataSource, List[StockDataSource]],
                 cache_dir: str, storage_format: str = 'parquet', enable_duckdb: bool = True)

    # 传统时间序列查询（单股票历史数据）
    def query_history_k_data_plus(self, code, fields, start_date, end_date, **kwargs) -> StockDataResult

    # 横截面查询（选股专用）
    def query_market_snapshot(self, date: str, filters: Dict = None) -> StockDataResult
    def query_technical_indicators(self, date: str, indicators: List[str] = None) -> StockDataResult
    def query_financial_snapshot(self, quarter: str) -> StockDataResult
    def query_fund_flow_data(self, date: str, days: int = 1) -> StockDataResult

    # 选股专用查询
    def screen_stocks(self, date: str, criteria: Dict, strategy_level: str = 'main') -> StockDataResult
    def get_stock_ranking(self, date: str, score_config: Dict, top_n: int = 50) -> StockDataResult

    # SQL查询接口
    def query_with_sql(self, sql: str, **params) -> StockDataResult

    # 数据源管理
    def add_datasource(self, datasource: StockDataSource, priority: int = 0) -> None
    def remove_datasource(self, datasource: StockDataSource) -> None
    def get_datasource_status(self) -> Dict[str, Any]

    # 内部方法
    def _calculate_missing_ranges(self, target_range, cached_ranges) -> List[Tuple[str, str]]
    def _merge_cached_data(self, query_key, start_date, end_date) -> StockDataResult
    def _build_technical_indicators(self, date: str) -> None  # 构建技术指标
    def _update_cross_sectional_cache(self, date: str) -> None  # 更新横截面缓存
```

**工作流程**：
1. 接收查询请求
2. 从元数据管理器获取已缓存范围
3. 计算需要从远程获取的数据范围
4. 通过多数据源管理器获取缺失数据（故障转移和负载均衡）
5. 将新数据存储到本地并更新元数据
6. 合并本地数据返回完整结果

#### 2. MetadataManager (metadata_manager.py)

**职责**：管理缓存数据的元数据索引

**核心功能**：
- 记录每个查询键（股票代码+字段组合）的已缓存日期范围
- 提供快速的范围查询接口
- 支持范围的合并和分割操作
- 元数据的持久化和恢复

**数据结构**：
```json
{
  "sh.600000_date,open,high,low,close_d_3": {
    "cached_ranges": [
      {"start": "2020-01-01", "end": "2022-12-31"},
      {"start": "2023-06-01", "end": "2023-12-31"}
    ],
    "last_updated": "2024-01-15T10:30:00",
    "fields": ["date", "open", "high", "low", "close"],
    "total_records": 1247
  }
}
```

#### 3. FileCache (file_cache.py)

**职责**：管理本地文件的存储和读取

**核心功能**：
- 按查询键组织文件存储结构
- 支持按时间范围的数据分片
- 提供数据的保存和加载接口
- 文件的压缩和解压缩

**存储结构**：
```
cache_dir/
├── metadata.json              # 全局元数据
├── data/
│   # 按股票代码分组（适合单股时间序列查询）
│   ├── by_stock/
│   │   ├── sh.600000/
│   │   │   ├── k_data_d_3/    # K线数据_日线_不复权
│   │   │   │   ├── 2020.parquet
│   │   │   │   ├── 2021.parquet
│   │   │   │   └── 2022.parquet
│   │   │   ├── financial_data/ # 财务数据
│   │   │   └── fund_flow_data/ # 资金流数据
│   │   └── sz.000001/
│   │       └── k_data_d_3/
│   # 按日期分组（适合横截面筛选查询）
│   └── by_date/
│       ├── market_data/       # 市场行情数据
│       │   ├── 2023-01-03.parquet  # 包含当日所有股票基础数据
│       │   ├── 2023-01-04.parquet
│       │   └── 2023-01-05.parquet
│       ├── technical_indicators/ # 技术指标数据
│       │   ├── 2023-01-03.parquet  # MA5/10/20, 换手率等
│       │   └── 2023-01-04.parquet
│       ├── financial_snapshot/  # 财务数据快照（按季度更新）
│       │   ├── 2023Q1.parquet   # 所有股票最新财务数据
│       │   └── 2023Q2.parquet
│       └── fund_flow/         # 资金流数据
│           ├── 2023-01-03.parquet
│           └── 2023-01-04.parquet
```

#### 4. RangeCalculator (range_calculator.py)

**职责**：日期范围的计算和操作

**核心算法**：
- 计算目标范围与已缓存范围的差集
- 合并重叠或相邻的日期范围
- 分割跨年度的查询范围
- 处理交易日历的特殊逻辑

**关键方法**：
```python
def calculate_missing_ranges(target_start: str, target_end: str, cached_ranges: List[DateRange]) -> List[DateRange]
def merge_ranges(ranges: List[DateRange]) -> List[DateRange]
def split_range_by_year(start: str, end: str) -> List[DateRange]
```

#### 5. Storage Backends (storage/)

**职责**：提供不同格式的数据存储实现

**支持格式**：
- **Parquet**：高性能列式存储，适合大数据量和复杂查询

**接口设计**：
```python
class BaseStorage(ABC):
    @abstractmethod
    def save(self, file_path: str, data: pd.DataFrame) -> bool

    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame

    @abstractmethod
    def exists(self, file_path: str) -> bool
```

#### 6. DuckDB Query Engine (query_engine.py)

**职责**：提供高性能的SQL查询引擎，支持复杂分析和聚合计算

**核心功能**：
- 自动创建和管理 DuckDB 数据库连接
- 将缓存的 Parquet 文件注册为 DuckDB 表
- 提供原生 SQL 查询接口
- 支持跨股票、跨时间的复杂分析查询
- 自动优化查询性能和内存使用

**核心方法**：
```python
class DuckDBQueryEngine:
    def __init__(self, cache_dir: str, memory_limit: str = "2GB")
    def register_cache_tables(self) -> None
    def execute_sql(self, sql: str, **params) -> pd.DataFrame
    def create_virtual_table(self, table_name: str, file_pattern: str) -> None
    def get_table_info(self, table_name: str) -> Dict[str, Any]
```

**表结构设计**：
```sql
-- K线数据表 (自动从缓存的Parquet文件创建)
CREATE VIEW k_data AS
SELECT * FROM 'cache_dir/data/*/k_data_*/*.parquet';

-- 财务数据表
CREATE VIEW financial_data AS
SELECT * FROM 'cache_dir/data/*/profit_data/*.parquet'
UNION ALL
SELECT * FROM 'cache_dir/data/*/balance_data/*.parquet';
```

**查询优化特性**：
- **列式存储优化**：充分利用 Parquet 的列式特性
- **谓词下推**：将过滤条件推送到存储层
- **并行查询**：自动并行化复杂查询
- **内存管理**：智能的内存使用和溢出处理

#### 7. Datasource Manager (datasource_manager.py)

**职责**：管理多个远程数据源，提供故障转移和负载均衡功能

**核心功能**：
- 数据源的注册、移除和优先级管理
- 自动故障检测和健康状态监控
- 智能故障转移和请求重试机制
- 负载均衡和请求分发策略
- 数据源性能统计和监控

**核心方法**：
```python
class DataSourceManager:
    def __init__(self, datasources: List[StockDataSource] = None)
    def add_datasource(self, datasource: StockDataSource, priority: int = 0, weight: int = 1) -> None
    def remove_datasource(self, datasource: StockDataSource) -> None
    def execute_query(self, method_name: str, *args, **kwargs) -> StockDataResult
    def get_healthy_datasources(self) -> List[StockDataSource]
    def check_datasource_health(self, datasource: StockDataSource) -> bool
    def get_statistics(self) -> Dict[str, Any]
```

**故障转移策略**：
```python
# 故障转移配置
failover_config = {
    'retry_count': 3,              # 单个数据源重试次数
    'retry_delay': 1.0,           # 重试延迟(秒)
    'circuit_breaker_threshold': 5, # 熔断阈值
    'circuit_breaker_timeout': 60,  # 熔断恢复时间(秒)
    'health_check_interval': 30,    # 健康检查间隔(秒)
}
```

**负载均衡策略**：
- **Round Robin**：轮询分发请求
- **Weighted Round Robin**：加权轮询，考虑数据源权重
- **Priority First**：优先级优先，高优先级数据源优先使用
- **Random**：随机选择数据源
- **Response Time**：基于响应时间选择最快的数据源

**数据源健康监控**：
```python
{
    "baostock": {
        "status": "healthy",
        "success_rate": 0.98,
        "avg_response_time": 2.3,
        "last_error": None,
        "circuit_breaker": "closed",
        "total_requests": 1247,
        "failed_requests": 25
    },
    "akshare": {
        "status": "degraded",
        "success_rate": 0.85,
        "avg_response_time": 5.1,
        "last_error": "Connection timeout",
        "circuit_breaker": "half_open",
        "total_requests": 892,
        "failed_requests": 134
    }
}
```

## 使用示例

### 基本使用

```python
from gupiao.datasource.baostock import BaostockDataSource
from gupiao.datasource.akshare import AkShareDataSource  # 假设有AkShare数据源
from gupiao.datasource.cache import CachedDataSource

# 创建多个远程数据源
baostock_ds = BaostockDataSource()
baostock_ds.login()

akshare_ds = AkShareDataSource()

# 方式1：使用单个数据源（向后兼容）
cached_ds = CachedDataSource(
    remote_datasources=baostock_ds,
    cache_dir="./stock_cache",
    storage_format="parquet",
    enable_duckdb=True
)

# 方式2：使用多个数据源（推荐）
cached_ds = CachedDataSource(
    remote_datasources=[baostock_ds, akshare_ds],
    cache_dir="./stock_cache",
    storage_format="parquet",
    enable_duckdb=True
)

# 第一次查询：从远程获取并缓存
result1 = cached_ds.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume",
    start_date="2023-01-01",
    end_date="2023-12-31"
)

# 第二次查询：直接从缓存读取
result2 = cached_ds.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume",
    start_date="2023-06-01",
    end_date="2023-08-31"  # 子范围，直接从缓存获取
)

# 扩展查询：部分从缓存，部分从远程
result3 = cached_ds.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume",
    start_date="2022-06-01",  # 2022年数据需要从远程获取
    end_date="2023-06-30"     # 2023年数据从缓存获取
)
```

### 多数据源高级配置

```python
# 高级多数据源配置
cached_ds = CachedDataSource(
    remote_datasources=[baostock_ds, akshare_ds],
    cache_dir="./stock_cache",
    storage_format="parquet",
    enable_duckdb=True,
    # 多数据源配置
    load_balance_strategy="priority_first",  # 负载均衡策略
    failover_enabled=True,                   # 启用故障转移
    circuit_breaker_enabled=True,            # 启用熔断器
    health_check_enabled=True                # 启用健康检查
)

# 动态添加数据源
tushare_ds = TushareDataSource()  # 假设有Tushare数据源
cached_ds.add_datasource(tushare_ds, priority=2, weight=1)

# 设置数据源优先级 (数字越小优先级越高)
# Priority 0: baostock (主要数据源)
# Priority 1: akshare (备用数据源)
# Priority 2: tushare (次要数据源)

# 查看数据源状态
status = cached_ds.get_datasource_status()
print("数据源状态:")
for name, info in status.items():
    print(f"  {name}: {info['status']} (成功率: {info['success_rate']:.2%})")

# 查询时自动使用最佳数据源
result = cached_ds.query_history_k_data_plus(
    code="sh.600000",
    fields="date,open,high,low,close,volume",
    start_date="2023-01-01",
    end_date="2023-12-31"
)
# 如果baostock失败，自动切换到akshare，再失败则切换到tushare

# 手动移除有问题的数据源
cached_ds.remove_datasource(akshare_ds)
```

### 选股策略专用查询

针对选股逻辑优化的横截面查询接口：

```python
# 1. 市场快照查询（某日所有股票基本数据）
market_snapshot = cached_ds.query_market_snapshot(
    date="2024-01-15",
    filters={
        "close": {"min": 5, "max": 100},      # 股价范围
        "market_value": {"min": 50e8, "max": 500e8},  # 市值范围50-500亿
        "turnover_rate": {"min": 0.01}       # 最小换手率1%
    }
)

# 2. 技术指标查询（预计算的技术指标）
tech_indicators = cached_ds.query_technical_indicators(
    date="2024-01-15",
    indicators=["ma5", "ma10", "ma20", "consecutive_up_days", "cumulative_return_3d"]
)

# 3. 财务数据快照（最新季度财务数据）
financial_data = cached_ds.query_financial_snapshot(quarter="2023Q4")

# 4. 资金流数据（近N日主力资金流）
fund_flow = cached_ds.query_fund_flow_data(date="2024-01-15", days=3)

# 5. 一站式选股筛选（核心功能）
selected_stocks = cached_ds.screen_stocks(
    date="2024-01-15",
    criteria={
        # 基本过滤
        "price_range": [5, 100],
        "market_value_range": [50e8, 500e8],

        # 趋势条件
        "consecutive_up_days": 3,
        "daily_return_min": 0.01,  # 每日涨幅>1%
        "cumulative_return_3d_max": 0.05,  # 3日累计涨幅<=5%

        # 均线条件
        "ma_alignment": "bull",  # MA5 > MA10 > MA20
        "close_above_ma5": True,

        # 换手率条件（分层）
        "turnover_strategy": "tier_based",  # 或 "relative" 或 "activity"
        "turnover_config": {
            "small_cap": {"daily": [0.05, 0.15], "cumulative_3d": 0.30},
            "mid_cap": {"daily": [0.02, 0.08], "cumulative_3d": 0.15},
            "large_cap": {"daily": [0.01, 0.03], "cumulative_3d": 0.08}
        },

        # 板块/大盘过滤
        "market_filter": {"hs300_trend": "positive", "above_ma20": True},
        "sector_filter": {"relative_strength": True, "fund_inflow": True}
    },
    strategy_level="main"  # 主策略
)

# 6. 分级回退筛选
if len(selected_stocks.to_dataframe()) == 0:
    # 回退1：放宽条件
    selected_stocks = cached_ds.screen_stocks(
        date="2024-01-15",
        criteria={...},  # 放宽的条件
        strategy_level="fallback1"
    )

# 7. 股票评分排序
ranked_stocks = cached_ds.get_stock_ranking(
    date="2024-01-15",
    score_config={
        "financial_factor": {"weight": 0.3, "metrics": ["roe", "revenue_growth", "debt_ratio"]},
        "fund_flow_factor": {"weight": 0.25, "metrics": ["net_inflow_3d", "volume_ratio"]},
        "stability_factor": {"weight": 0.25, "metrics": ["volatility_3d", "amplitude"]},
        "price_position_factor": {"weight": 0.2, "window": 252}
    },
    top_n=50
)
```

### DuckDB SQL 查询

启用 DuckDB 后，可以使用 SQL 进行复杂的数据分析：

```python
# 1. 选股筛选SQL（横截面查询）
selected_stocks = cached_ds.query_with_sql("""
    SELECT
        m.code, m.name, m.close, m.market_value, m.turnover_rate,
        t.ma5, t.ma10, t.ma20, t.consecutive_up_days, t.cumulative_return_3d,
        f.roe, f.revenue_growth, f.debt_ratio,
        ff.net_inflow_3d, ff.volume_ratio
    FROM market_data m
    JOIN technical_indicators t ON m.code = t.code AND m.date = t.date
    JOIN financial_snapshot f ON m.code = f.code
    JOIN fund_flow ff ON m.code = ff.code AND m.date = ff.date
    WHERE m.date = '2024-01-15'
        AND m.close BETWEEN 5 AND 100
        AND m.market_value BETWEEN 50e8 AND 500e8
        AND t.consecutive_up_days >= 3
        AND t.ma5 > t.ma10 AND t.ma10 > t.ma20
        AND m.close > t.ma5
        AND t.cumulative_return_3d <= 0.05
        AND (
            (m.market_value < 100e8 AND m.turnover_rate BETWEEN 0.05 AND 0.15) OR
            (m.market_value BETWEEN 100e8 AND 500e8 AND m.turnover_rate BETWEEN 0.02 AND 0.08) OR
            (m.market_value > 500e8 AND m.turnover_rate BETWEEN 0.01 AND 0.03)
        )
    ORDER BY
        f.roe * 0.3 + ff.net_inflow_3d * 0.25 + (1-t.volatility_3d) * 0.25 +
        (1-m.price_position_252d) * 0.2 DESC
    LIMIT 50
""")

# 2. 聚合分析查询
monthly_stats = cached_ds.query_with_sql("""
    SELECT
        code,
        DATE_TRUNC('month', date) as month,
        AVG(close) as avg_price,
        MAX(high) as max_price,
        MIN(low) as min_price,
        SUM(volume) as total_volume
    FROM k_data
    WHERE code IN ('sh.600000', 'sh.600036', 'sz.000001')
    AND date >= '2023-01-01'
    GROUP BY code, DATE_TRUNC('month', date)
    ORDER BY code, month
""")

# 3. 技术指标计算
ma_result = cached_ds.query_with_sql("""
    SELECT
        code, date, close,
        AVG(close) OVER (
            PARTITION BY code
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as ma20,
        AVG(close) OVER (
            PARTITION BY code
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) as ma5
    FROM k_data
    WHERE code = 'sh.600000'
    AND date >= '2023-01-01'
    ORDER BY date
""")

# 4. 跨表关联查询（K线 + 财务数据）
combined_analysis = cached_ds.query_with_sql("""
    SELECT
        k.code, k.date, k.close,
        f.revenue, f.profit, f.roe
    FROM k_data k
    LEFT JOIN financial_data f
        ON k.code = f.code
        AND DATE_TRUNC('quarter', k.date) = f.report_date
    WHERE k.code = 'sh.600000'
    AND k.date >= '2023-01-01'
    ORDER BY k.date
""")

# 5. 参数化查询
parameterized_result = cached_ds.query_with_sql("""
    SELECT code, date, close
    FROM k_data
    WHERE code = $code
    AND date BETWEEN $start_date AND $end_date
    AND close > $min_price
""", code='sh.600000', start_date='2023-01-01', end_date='2023-12-31', min_price=10.0)
```

### 高级配置

```python
# 自定义存储配置
cached_ds = CachedDataSource(
    remote_datasources=[baostock_ds, akshare_ds, tushare_ds],
    cache_dir="./stock_cache",
    storage_format="parquet",
    compression="snappy",        # 压缩算法
    max_cache_size_gb=10,       # 最大缓存大小
    auto_cleanup=True,          # 自动清理过期数据
    metadata_backup_interval=100, # 元数据备份间隔
    # DuckDB 配置
    enable_duckdb=True,         # 启用DuckDB查询引擎
    duckdb_memory_limit="4GB",  # DuckDB内存限制
    duckdb_threads=4,           # 查询并行线程数
    auto_register_tables=True,  # 自动注册缓存表
    # 多数据源配置
    load_balance_strategy="priority_first",  # 负载均衡策略
    failover_enabled=True,                   # 启用故障转移
    circuit_breaker_enabled=True,            # 启用熔断器
    health_check_enabled=True,               # 启用健康检查
    max_concurrent_requests=5,               # 最大并发请求数
    request_timeout=30                       # 请求超时时间(秒)
)

# 查看缓存统计
stats = cached_ds.get_cache_stats()
print(f"缓存命中率: {stats['hit_rate']:.2%}")
print(f"存储大小: {stats['storage_size_mb']:.1f} MB")
print(f"数据记录数: {stats['total_records']:,}")
```

## 性能特性

### 查询性能

| 场景 | 传统方式 | 缓存方式 | DuckDB查询 | 提升倍数 |
|------|----------|----------|------------|----------|
| 重复查询 | 5-10秒 | 0.1-0.5秒 | 0.05-0.2秒 | 25-200x |
| 部分重叠查询 | 5-10秒 | 1-3秒 | 0.5-1.5秒 | 3-20x |
| 扩展查询 | 5-10秒 | 2-4秒 | 1-2秒 | 2.5-10x |
| 聚合计算 | 10-30秒 | 5-15秒 | 0.5-2秒 | 5-60x |
| 跨表关联 | 20-60秒 | 10-30秒 | 1-5秒 | 4-60x |
| 技术指标计算 | 15-45秒 | 8-20秒 | 0.8-3秒 | 5-56x |
| **选股筛选(4000股)** | **60-180秒** | **30-90秒** | **2-8秒** | **7.5-90x** |
| **市场快照查询** | **30-120秒** | **15-60秒** | **0.5-3秒** | **10-240x** |
| **分级回退筛选** | **120-300秒** | **60-150秒** | **5-15秒** | **8-60x** |
| **多因子评分排序** | **180-600秒** | **90-300秒** | **8-25秒** | **7.5-75x** |

### 存储效率

- **Parquet格式**：列式存储，高效压缩和查询性能
- **数据压缩**：Snappy压缩比例约50-70%
- **索引优化**：元数据大小通常<1%数据大小

### 网络优化

- **增量下载**：只下载缺失的数据范围
- **批量请求**：合并相邻的小范围请求
- **智能预取**：根据查询模式预加载数据

## 配置选项

### 存储配置

```python
storage_config = {
    'compression': 'snappy',       # 压缩算法: snappy, gzip, lz4
    'partition_by': 'year',        # 分区策略: year, quarter, month
    'max_file_size_mb': 100,       # 单文件最大大小
}
```

### 缓存策略

```python
cache_config = {
    'max_cache_size_gb': 10,       # 最大缓存大小
    'auto_cleanup': True,          # 自动清理
    'cleanup_threshold': 0.9,      # 清理触发阈值
    'retention_days': 365,         # 数据保留天数
}
```

### 性能调优

```python
performance_config = {
    'parallel_downloads': 3,       # 并发下载数
    'chunk_size_days': 30,        # 单次请求天数
    'preload_adjacent': True,     # 预加载相邻数据
    'memory_cache_size_mb': 500,  # 内存缓存大小
}
```

### DuckDB 查询优化

```python
duckdb_config = {
    'memory_limit': '4GB',         # 内存限制
    'threads': 4,                  # 并行线程数
    'max_memory': '80%',          # 最大内存使用率
    'temp_directory': './tmp',     # 临时文件目录
    'enable_optimizer': True,      # 启用查询优化器
    'enable_profiling': False,     # 查询性能分析
    'checkpoint_threshold': '1GB', # 检查点触发阈值
}
```

### 多数据源配置

```python
multi_datasource_config = {
    'load_balance_strategy': 'priority_first',  # 负载均衡策略
    'failover_enabled': True,                   # 启用故障转移
    'retry_count': 3,                          # 重试次数
    'retry_delay': 1.0,                        # 重试延迟(秒)
    'circuit_breaker_enabled': True,           # 启用熔断器
    'circuit_breaker_threshold': 5,            # 熔断阈值
    'circuit_breaker_timeout': 60,             # 熔断恢复时间(秒)
    'health_check_enabled': True,              # 启用健康检查
    'health_check_interval': 30,               # 健康检查间隔(秒)
    'max_concurrent_requests': 5,              # 最大并发请求数
    'request_timeout': 30,                     # 请求超时时间(秒)
    'datasource_weights': {                    # 数据源权重配置
        'baostock': 1.0,
        'akshare': 0.8,
        'tushare': 0.6
    },
    'datasource_priorities': {                 # 数据源优先级配置
        'baostock': 0,   # 最高优先级
        'akshare': 1,
        'tushare': 2
    }
}
```

## 错误处理和恢复

### 常见错误场景

1. **元数据损坏**：自动从备份恢复或重建索引
2. **数据文件缺失**：标记为需要重新下载
3. **单个数据源失败**：自动切换到备用数据源
4. **所有数据源失败**：返回已缓存的部分数据，记录错误日志
5. **数据源被反爬限制**：触发熔断器，暂时停用该数据源
6. **网络连接不稳定**：自动重试和指数退避
7. **磁盘空间不足**：自动清理或降级存储格式
8. **数据源返回数据不一致**：数据校验和源优先级选择

### 数据一致性保证

- **原子操作**：数据写入和元数据更新的原子性
- **校验机制**：数据完整性校验和自动修复
- **备份策略**：元数据的多版本备份
- **恢复机制**：从异常状态的自动恢复

## 扩展性设计

### 插件化存储后端

```python
# 自定义存储后端
class RedisStorage(BaseStorage):
    def save(self, key: str, data: pd.DataFrame) -> bool:
        # Redis存储实现
        pass

    def load(self, key: str) -> pd.DataFrame:
        # Redis读取实现
        pass

# 注册新的存储后端
CachedDataSource.register_storage('redis', RedisStorage)
```

### 缓存策略扩展

```python
# 自定义缓存策略
class LRUCacheStrategy(BaseCacheStrategy):
    def should_cache(self, query_info: QueryInfo) -> bool:
        # 自定义缓存决策逻辑
        pass

    def evict_candidates(self, cache_info: CacheInfo) -> List[str]:
        # 自定义淘汰策略
        pass
```

### 监控和观察

```python
# 集成监控
cached_ds.add_monitor(PrometheusMonitor())
cached_ds.add_monitor(LoggingMonitor())

# 事件钩子
@cached_ds.on_cache_hit
def on_cache_hit(query_info, execution_time):
    print(f"Cache hit for {query_info.code}: {execution_time}ms")

@cached_ds.on_remote_fetch
def on_remote_fetch(query_info, data_size):
    print(f"Remote fetch for {query_info.code}: {data_size} records")
```

## 最佳实践

### 1. 选股策略数据源配置

```python
# 针对选股策略的数据源配置
# Baostock: 行情数据(开高低收、成交量、换手率、市值)
# AkShare: 财务数据(ROE、营收增长等) + 资金流数据

baostock_ds = BaostockDataSource()
akshare_ds = AkShareDataSource()

cached_ds = CachedDataSource(
    remote_datasources=[baostock_ds, akshare_ds],
    cache_dir="./stock_selection_cache",
    storage_format="parquet",
    enable_duckdb=True,
    # 选股专用配置
    enable_cross_sectional_cache=True,  # 启用横截面缓存
    precompute_technical_indicators=True,  # 预计算技术指标
    daily_update_time="15:30",  # 每日更新时间(收盘后)
)

# 预计算技术指标配置
tech_indicators_config = {
    "ma_periods": [5, 10, 20, 60],  # 移动平均线周期
    "consecutive_days": True,        # 连续上涨天数
    "cumulative_returns": [3, 5, 10], # 累计收益率天数
    "volatility_windows": [3, 5, 20], # 波动率计算窗口
    "price_position_windows": [60, 252] # 价格位置窗口
}
```

### 2. 选股缓存预热策略

```python
# 选股策略的缓存预热
from datetime import datetime, timedelta

# 1. 预热市场数据（最近6个月所有股票）
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

# 预热基础行情数据
cached_ds.preload_market_data(
    start_date=start_date,
    end_date=end_date,
    fields=["open", "high", "low", "close", "volume", "turnover", "market_value"]
)

# 预热技术指标数据
cached_ds.precompute_technical_indicators(
    start_date=start_date,
    end_date=end_date,
    indicators_config=tech_indicators_config
)

# 预热财务数据快照
cached_ds.preload_financial_snapshots(quarters=["2023Q3", "2023Q4", "2024Q1"])

# 预热资金流数据
cached_ds.preload_fund_flow_data(
    start_date=start_date,
    end_date=end_date,
    metrics=["net_inflow", "main_inflow", "volume_ratio"]
)
```

### 3. 选股策略定期维护

```python
# 每日收盘后的维护任务
def daily_maintenance():
    # 1. 更新最新交易日数据
    today = datetime.now().strftime('%Y-%m-%d')
    cached_ds.update_daily_data(date=today)

    # 2. 重新计算技术指标
    cached_ds.rebuild_technical_indicators(date=today)

    # 3. 更新横截面缓存
    cached_ds.refresh_cross_sectional_cache(date=today)

    # 4. 检查数据质量
    quality_report = cached_ds.check_data_quality(date=today)
    if quality_report['missing_stocks'] > 100:
        print(f"警告: {quality_report['missing_stocks']} 只股票数据缺失")

    # 5. 优化存储
    cached_ds.optimize_storage()

# 每周的深度维护
def weekly_maintenance():
    # 1. 更新财务数据快照（如果有新财报）
    cached_ds.update_financial_snapshots()

    # 2. 重建技术指标索引
    cached_ds.rebuild_technical_indicators_index()

    # 3. 清理过期缓存
    cached_ds.cleanup_expired_cache(retention_days=180)

    # 4. 数据源健康检查
    status = cached_ds.get_datasource_status()
    for name, info in status.items():
        if info['success_rate'] < 0.8:
            print(f"警告: {name} 数据源成功率过低: {info['success_rate']:.2%}")
```

### 4. 监控缓存效果

```python
# 定期检查缓存性能
stats = cached_ds.get_detailed_stats()
if stats['hit_rate'] < 0.7:
    print("Cache hit rate is low, consider adjusting cache strategy")

# 监控数据源使用分布
datasource_stats = cached_ds.get_datasource_statistics()
print("数据源使用统计:")
for name, stat in datasource_stats.items():
    print(f"  {name}: {stat['request_count']} 次请求, 平均响应时间 {stat['avg_response_time']:.2f}s")
```

## 迁移指南

### 从现有代码迁移

1. **替换数据源创建**：
```python
# 原来的代码
ds = BaostockDataSource()

# 迁移后的代码
ds = CachedDataSource(BaostockDataSource(), cache_dir="./cache")
```

2. **保持API兼容**：所有现有的查询代码无需修改

3. **渐进式迁移**：可以在新功能中先使用缓存数据源，现有功能保持不变

本架构设计充分考虑了历史股票数据的特点，通过智能的元数据管理和范围查询优化，为股票数据分析提供了高效、可靠的缓存解决方案。