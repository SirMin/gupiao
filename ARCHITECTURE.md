# 股票选择策略系统架构文档

## 1. 系统概述

### 1.1 项目描述
基于 Baostock 数据源和 Backtrader 回测框架的A股选股策略系统，通过规则筛选 + 评分排序的双层机制，每日输出候选股票列表。

### 1.2 核心目标
- 抓取"温和上升且资金支撑、基本面相对稳健"的中短线候选股
- 严格优先主策略，找不到才逐级回退
- 提供可配置、可扩展的评分引擎

### 1.3 技术栈
- **数据源**: Baostock
- **回测框架**: Backtrader
- **评分引擎**: 自定义 ScoreEngine
- **开发语言**: Python
- **配置管理**: YAML/JSON

## 2. 系统架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    股票选择策略系统                          │
├─────────────────────────────────────────────────────────────┤
│  Runner (每日执行调度器)                                     │
├─────────────┬─────────────┬─────────────┬─────────────────────┤
│ DataLoader  │ Preprocessor│ StrategyEngine │ ScoreEngine      │
│ (数据加载)   │ (数据预处理) │ (策略筛选)     │ (评分排序)        │
├─────────────┼─────────────┼─────────────┼─────────────────────┤
│ Baostock    │ 指标计算     │ 主策略       │ FinancialFactor     │
│ API         │ 数据补全     │ 回退策略1     │ FundFlowFactor      │
│             │ 数据合并     │ 回退策略2     │ StabilityFactor     │
│             │             │ 兜底策略     │ PricePositionFactor │
├─────────────┴─────────────┴─────────────┴─────────────────────┤
│  Reporter (结果输出)                                         │
│  - CSV/Excel 输出                                           │
│  - 可视化报告                                               │
│  - 日志记录                                                 │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块层次结构

```
src/
├── core/                      # 核心业务逻辑
│   ├── data_loader.py         # 数据加载模块
│   ├── preprocessor.py        # 数据预处理模块
│   ├── strategy_engine.py     # 策略筛选引擎
│   ├── score_engine.py        # 评分引擎
│   └── runner.py              # 主运行器
├── factors/                   # 评分因子
│   ├── base_factor.py         # 因子基类
│   ├── financial_factor.py    # 财务因子
│   ├── fund_flow_factor.py    # 资金面因子
│   ├── stability_factor.py    # 稳定性因子
│   └── price_position_factor.py # 价格位置因子
├── strategies/                # 策略规则
│   ├── base_strategy.py       # 策略基类
│   ├── main_strategy.py       # 主策略
│   ├── fallback_strategies.py # 回退策略
│   └── emergency_strategy.py  # 兜底策略
├── utils/                     # 工具模块
│   ├── config.py              # 配置管理
│   ├── logger.py              # 日志工具
│   └── helpers.py             # 辅助函数
├── reporters/                 # 报告生成
│   ├── csv_reporter.py        # CSV输出
│   ├── excel_reporter.py      # Excel输出
│   └── visualizer.py          # 可视化
└── tests/                     # 单元测试
    ├── test_data_loader.py
    ├── test_strategy_engine.py
    ├── test_score_engine.py
    └── test_factors.py
```

## 3. 核心模块设计

### 3.1 数据加载模块 (DataLoader)

#### 职责
- 从 Baostock 获取日线行情数据
- 处理数据连接和异常
- 提供数据缓存机制

#### 接口设计
```python
class DataLoader:
    def __init__(self, config):
        pass

    def get_stock_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取单只股票历史数据"""
        pass

    def get_all_stocks_data(self, date: str) -> pd.DataFrame:
        """获取指定日期所有股票数据"""
        pass

    def get_market_data(self, index_code: str, date: str) -> pd.DataFrame:
        """获取大盘/板块数据"""
        pass
```

### 3.2 数据预处理模块 (Preprocessor)

#### 职责
- 数据清洗和补全
- 技术指标计算 (MA5, MA10, MA20, 换手率等)
- 数据格式标准化

#### 接口设计
```python
class Preprocessor:
    def __init__(self, config):
        pass

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        pass

    def calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算移动平均线"""
        pass

    def calculate_turnover_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算换手率相关指标"""
        pass
```

### 3.3 策略筛选引擎 (StrategyEngine)

#### 职责
- 实现主策略和多级回退机制
- 管理筛选规则的执行顺序
- 提供策略配置接口

#### 架构设计
```python
class StrategyEngine:
    def __init__(self, config):
        self.strategies = [
            MainStrategy(config),
            FallbackStrategy1(config),
            FallbackStrategy2(config),
            EmergencyStrategy(config)
        ]

    def filter_stocks(self, data: pd.DataFrame) -> pd.DataFrame:
        """执行分级筛选策略"""
        for i, strategy in enumerate(self.strategies):
            candidates = strategy.apply(data)
            if len(candidates) > 0:
                candidates['strategy_level'] = i
                return candidates
        return pd.DataFrame()  # 理论上不会到这里
```

#### 策略层级设计

1. **主策略 (MainStrategy)**
   - 基本过滤: 价格 < 100元, 市值 [50亿, 500亿]
   - 趋势条件: 连续上涨3天, 3日累计涨幅控制
   - 均线条件: 价格>MA5, MA5>MA10>MA20
   - 换手率: 三方案择一 (分层区间/相对换手/活跃度)
   - 板块/大盘: 辅助过滤条件

2. **回退策略1 (FallbackStrategy1)**
   - 连续上涨天数降为2
   - 均线放宽为 MA5>MA10
   - 换手率条件放宽
   - 累计涨幅上限+20%

3. **回退策略2 (FallbackStrategy2)**
   - 不强制连续上涨, 改为当日收盘>MA10且当日上涨
   - 换手率只需高于行业中位数

4. **兜底策略 (EmergencyStrategy)**
   - 仅要求: 市值范围 + 股价<100元
   - 依赖评分引擎排序

### 3.4 评分引擎 (ScoreEngine)

#### 职责
- 管理多个评分因子
- 执行因子计算和权重合成
- 提供可配置的权重调整

#### 架构设计
```python
class ScoreEngine:
    def __init__(self, config):
        self.factors = {
            'financial': FinancialFactor(config.get('financial_weight', 0.3)),
            'fund_flow': FundFlowFactor(config.get('fund_flow_weight', 0.25)),
            'stability': StabilityFactor(config.get('stability_weight', 0.25)),
            'price_position': PricePositionFactor(config.get('price_pos_weight', 0.2))
        }

    def calculate_scores(self, candidates: pd.DataFrame) -> pd.DataFrame:
        """计算综合评分"""
        for name, factor in self.factors.items():
            candidates[f'{name}_score'] = factor.calculate(candidates)

        # 计算加权综合分
        candidates['total_score'] = self._weighted_sum(candidates)
        return candidates.sort_values('total_score', ascending=False)
```

#### 因子设计

1. **财务因子 (FinancialFactor)**
   - ROE、营收增长、净利润同比
   - 负债率、自由现金流
   - 权重建议: 0.25-0.35

2. **资金面因子 (FundFlowFactor)**
   - 近3日主力净买入占流通市值比
   - 量比、成交额比
   - 权重建议: 0.2-0.3

3. **稳定性因子 (StabilityFactor)**
   - 最近3日日振幅、收盘价标准差
   - 极端单日涨跌检测(>5%)
   - 权重建议: 0.2-0.3

4. **价格位置因子 (PricePositionFactor)**
   - 当前价在过去252日区间的位置
   - 低位得高分的设计
   - 权重建议: 0.1-0.2

## 4. 数据流设计

### 4.1 数据流转图

```
Baostock API
     ↓
[DataLoader] → 原始行情数据
     ↓
[Preprocessor] → 计算技术指标
     ↓
[StrategyEngine] → 分级筛选
     ↓ (候选股票)
[ScoreEngine] → 评分排序
     ↓
[Reporter] → 输出结果
```

### 4.2 数据模型设计

#### 股票数据模型
```python
StockData = {
    'code': str,           # 股票代码
    'name': str,           # 股票名称
    'date': datetime,      # 交易日期
    'open': float,         # 开盘价
    'high': float,         # 最高价
    'low': float,          # 最低价
    'close': float,        # 收盘价
    'volume': int,         # 成交量
    'amount': float,       # 成交额
    'turn': float,         # 换手率
    'market_value': float, # 总市值
    'ma5': float,          # 5日均线
    'ma10': float,         # 10日均线
    'ma20': float,         # 20日均线
    'consecutive_days': int, # 连续上涨天数
    'return_3d': float,    # 3日累计收益率
    'avg_turn_60d': float  # 60日平均换手率
}
```

#### 输出结果模型
```python
CandidateStock = {
    'code': str,              # 股票代码
    'name': str,              # 股票名称
    'date': datetime,         # 筛选日期
    'total_score': float,     # 综合评分
    'fin_score': float,       # 财务因子得分
    'fund_score': float,      # 资金面因子得分
    'stab_score': float,      # 稳定性因子得分
    'price_pos_score': float, # 价格位置因子得分
    'consecutive_days': int,  # 连续上涨天数
    'return_3d': float,       # 3日收益率
    'ma_status': str,         # 均线状态
    'turnover_1d': float,     # 单日换手率
    'turnover_3d': float,     # 3日累计换手率
    'market_value': float,    # 总市值
    'close': float,           # 收盘价
    'volume': int,            # 成交量
    'strategy_level': int     # 策略级别 (0:主策略, 1:回退1, 2:回退2, 3:兜底)
}
```

## 5. 配置管理设计

### 5.1 配置文件结构 (config.yaml)

```yaml
# 数据源配置
data_source:
  provider: "baostock"
  cache_enabled: true
  cache_path: "./data/cache/"

# 基本筛选参数
basic_filter:
  max_price: 100.0
  min_market_value: 5000000000  # 50亿
  max_market_value: 50000000000 # 500亿

# 主策略参数
main_strategy:
  consecutive_days: 3
  daily_return_thresholds:
    small_cap: 0.015  # 1.5%
    mid_cap: 0.01     # 1.0%
    large_cap: 0.005  # 0.5%
  cumulative_return_limits:
    small_cap: 0.08   # 8%
    mid_cap: 0.05     # 5%
    large_cap: 0.03   # 3%

  # 换手率配置
  turnover_strategy: "tiered"  # tiered/relative/activity
  turnover_config:
    tiered:
      small_cap: {min: 0.05, max: 0.15, cum_max: 0.30}
      mid_cap: {min: 0.02, max: 0.08, cum_max: 0.15}
      large_cap: {min: 0.01, max: 0.03, cum_max: 0.08}

# 回退策略参数
fallback_strategies:
  level1:
    consecutive_days: 2
    ma_condition: "ma5_gt_ma10"  # 放宽均线条件
    return_limit_boost: 0.2      # 涨幅上限放宽20%

  level2:
    require_consecutive: false
    ma_condition: "close_gt_ma10_and_up_today"
    turnover_strategy: "industry_median"

# 评分引擎配置
score_engine:
  factors:
    financial:
      weight: 0.30
      enabled: true
    fund_flow:
      weight: 0.25
      enabled: true
    stability:
      weight: 0.25
      enabled: true
    price_position:
      weight: 0.20
      enabled: true

  normalization_method: "min_max"  # min_max/z_score

# 输出配置
output:
  max_candidates: 20
  formats: ["csv", "excel", "json"]
  output_path: "./results/"
  include_visualization: true

# 日志配置
logging:
  level: "INFO"
  file_path: "./logs/stock_selector.log"
  max_file_size: "10MB"
  backup_count: 5
```

## 6. 扩展性设计

### 6.1 因子扩展机制

#### 基础因子接口
```python
from abc import ABC, abstractmethod

class BaseFactor(ABC):
    def __init__(self, weight: float):
        self.weight = weight

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """计算因子值，返回0-1之间的标准化分数"""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """返回因子名称"""
        pass

    def validate_data(self, data: pd.DataFrame) -> bool:
        """验证输入数据完整性"""
        return True
```

#### 自定义因子示例
```python
class CustomMomentumFactor(BaseFactor):
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        # 自定义动量因子计算逻辑
        momentum_score = (data['close'] / data['close'].shift(20) - 1) * 100
        return self._normalize(momentum_score)

    def get_name(self) -> str:
        return "custom_momentum"
```

### 6.2 策略扩展机制

#### 基础策略接口
```python
class BaseStrategy(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用策略筛选，返回符合条件的股票"""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """返回策略名称"""
        pass
```

### 6.3 数据源扩展

#### 数据源接口
```python
class BaseDataSource(ABC):
    @abstractmethod
    def get_stock_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_all_stocks(self, date: str) -> list:
        pass
```

## 7. 性能优化设计

### 7.1 数据缓存策略
- 本地文件缓存历史数据
- Redis 缓存实时计算结果
- 增量更新机制

### 7.2 并行计算
- 多进程处理股票池筛选
- 异步IO优化数据获取
- 因子计算向量化

### 7.3 内存管理
- 分批处理大数据集
- 及时释放中间结果
- 使用生成器减少内存占用

## 8. 监控与告警

### 8.1 系统监控指标
- 数据获取成功率
- 策略执行时间
- 候选股票数量分布
- 因子计算异常率

### 8.2 业务监控指标
- 每日候选股票质量
- 策略回退触发频率
- 评分分布合理性
- 历史候选股票表现

### 8.3 告警机制
- 数据源异常告警
- 候选池为空告警
- 因子计算异常告警
- 系统性能告警

## 9. 部署架构

### 9.1 开发环境
```
开发机 → Git Repository → 本地测试
```

### 9.2 生产环境
```
定时任务服务器 → 数据库 → 结果存储 → 通知系统
```

### 9.3 容器化部署
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

CMD ["python", "-m", "src.core.runner"]
```

## 10. 安全性考虑

### 10.1 数据安全
- API密钥加密存储
- 数据传输加密
- 敏感信息脱敏

### 10.2 系统安全
- 输入参数验证
- SQL注入防护
- 异常处理机制

## 11. 测试策略

### 11.1 单元测试
- 每个因子独立测试
- 策略规则逻辑测试
- 数据处理函数测试

### 11.2 集成测试
- 端到端流程测试
- 数据源连接测试
- 配置加载测试

### 11.3 回测验证
- 历史数据回测
- 不同市场环境测试
- 参数敏感性测试

## 12. 文档与维护

### 12.1 技术文档
- API接口文档
- 配置参数说明
- 部署运维手册

### 12.2 业务文档
- 策略逻辑说明
- 因子计算方法
- 风控规则说明

这个架构设计提供了一个完整、可扩展、可维护的股票选择策略系统框架，支持快速开发和持续优化。