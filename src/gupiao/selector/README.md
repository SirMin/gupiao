# 股票选择策略系统

基于 Baostock 数据源的A股选股策略系统，通过多级回退策略筛选和多因子评分模型，每日输出高质量候选股票。

## 核心特性

### 🎯 多级策略筛选
- **主策略**: 严格筛选条件（连续上涨、均线多头、换手率控制）
- **回退策略**: 分级放宽条件，保证每日有候选股票
- **兜底策略**: 最宽松条件，确保系统稳定运行

### 📊 多因子评分系统
- **财务因子**: ROE、营收增长、净利润增长、负债率、现金流
- **资金面因子**: 量比、成交额活跃度、换手率、主力资金流向
- **稳定性因子**: 日振幅、波动率、极端波动惩罚、价格连续性
- **价格位置因子**: 历史区间位置、相对均价、回调幅度分析

### ⚙️ 灵活配置管理
- YAML配置文件，支持热更新
- 策略参数可调节
- 因子权重可配置
- 多种输出格式

## 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <repository_url>
cd stock_selector

# 安装依赖
pip install -r requirements.txt

# 创建必要目录
mkdir -p logs results data/cache
```

### 2. 基础使用

```bash
# 使用默认配置进行单日选股
python main.py --date 2024-01-15

# 使用简化配置（适合测试）
python main.py --date 2024-01-15 --config config/simple_config.yaml

# 查看详细输出
python main.py --date 2024-01-15 --verbose
```

### 3. 批量选股

```bash
# 批量处理一周的数据
python main.py --batch --start-date 2024-01-08 --end-date 2024-01-12

# 指定输出目录
python main.py --batch --start-date 2024-01-08 --end-date 2024-01-12 --output ./custom_results/
```

### 4. 指定股票池

```bash
# 只对指定股票进行选股
python main.py --date 2024-01-15 --stocks sh.600000,sz.000001,sz.000002
```

## 项目结构

```
stock_selector/
├── src/                          # 源代码
│   ├── core/                     # 核心模块
│   │   ├── preprocessor.py       # 数据预处理
│   │   ├── strategy_engine.py    # 策略引擎
│   │   ├── score_engine.py       # 评分引擎
│   │   └── runner.py             # 主运行器
│   ├── factors/                  # 评分因子
│   │   ├── base_factor.py        # 因子基类
│   │   ├── financial_factor.py   # 财务因子
│   │   ├── fund_flow_factor.py   # 资金面因子
│   │   ├── stability_factor.py   # 稳定性因子
│   │   └── price_position_factor.py # 价格位置因子
│   ├── strategies/               # 筛选策略
│   │   ├── base_strategy.py      # 策略基类
│   │   ├── main_strategy.py      # 主策略
│   │   └── fallback_strategies.py # 回退策略
│   ├── utils/                    # 工具模块
│   │   ├── config.py             # 配置管理
│   │   ├── logger.py             # 日志工具
│   │   └── helpers.py            # 辅助函数
│   └── reporters/                # 报告生成
│       └── csv_reporter.py       # CSV报告器
├── config/                       # 配置文件
│   ├── default_config.yaml       # 默认配置
│   └── simple_config.yaml        # 简化配置
├── main.py                       # 程序入口
├── requirements.txt              # 依赖清单
└── README.md                     # 项目说明
```

## 配置说明

### 主要配置项

```yaml
# 基础筛选参数
basic_filter:
  max_price: 100.0                    # 最大股价
  min_market_value: 5000000000        # 最小市值(50亿)
  max_market_value: 50000000000       # 最大市值(500亿)

# 主策略参数
main_strategy:
  consecutive_days: 3                 # 连续上涨天数
  max_cumulative_return: 0.05         # 3日累计涨幅上限
  turnover_strategy: "tiered"         # 换手率策略

# 评分引擎
score_engine:
  factors:
    financial:
      weight: 0.30                    # 财务因子权重
      enabled: true
    fund_flow:
      weight: 0.25                    # 资金面因子权重
    stability:
      weight: 0.25                    # 稳定性因子权重
    price_position:
      weight: 0.20                    # 价格位置因子权重
```

### 配置文件类型

- `default_config.yaml`: 完整配置文件，包含所有参数
- `simple_config.yaml`: 简化配置，适合快速测试

## 输出结果

### CSV报告格式

| 字段 | 说明 |
|------|------|
| 股票代码 | 标准格式代码 |
| 股票名称 | 股票简称 |
| 收盘价 | 目标日期收盘价 |
| 综合评分 | 多因子综合得分 |
| 连续上涨天数 | 连续上涨交易日数 |
| 3日收益率(%) | 近3日累计收益率 |
| 换手率(%) | 当日换手率 |
| 市值(亿) | 总市值 |
| 策略级别 | 使用的策略等级 |
| 各因子得分 | 财务、资金面、稳定性、价格位置得分 |

### 日志输出

```
2024-01-15 15:30:01 - INFO - 开始执行选股流程，目标日期: 2024-01-15
2024-01-15 15:30:05 - INFO - 获取到 4500 只股票的数据
2024-01-15 15:30:08 - INFO - 有效股票数: 4200
2024-01-15 15:30:12 - INFO - 策略筛选完成，使用策略: 主策略, 候选股票: 35 只
2024-01-15 15:30:15 - INFO - 综合评分计算完成
2024-01-15 15:30:16 - INFO - CSV报告生成成功: ./results/stock_candidates_2024-01-15.csv
```

## 策略逻辑

### 筛选流程

1. **数据获取**: 从 Baostock 获取A股日线数据
2. **数据预处理**: 计算技术指标（均线、换手率、连续涨跌等）
3. **基础过滤**: 价格、市值、成交量基本条件
4. **策略筛选**: 按优先级执行多级策略
5. **评分排序**: 多因子模型综合评分
6. **结果输出**: 生成报告文件

### 回退机制

- **主策略**: 连续上涨3天 + 多头排列 + 换手率适中
- **回退策略1**: 连续上涨2天 + MA5>MA10 + 换手率放宽
- **回退策略2**: 当日上涨 + 收盘>MA10 + 换手率宽松
- **兜底策略**: 仅基础条件 + 评分排序

## 高级用法

### 自定义因子

```python
from src.factors.base_factor import BaseFactor

class CustomFactor(BaseFactor):
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        # 实现自定义因子逻辑
        return self.normalize(custom_scores)

    def get_name(self) -> str:
        return "自定义因子"
```

### 自定义策略

```python
from src.strategies.base_strategy import BaseStrategy

class CustomStrategy(BaseStrategy):
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        # 实现自定义策略逻辑
        return filtered_data

    def get_name(self) -> str:
        return "自定义策略"
```

## 性能优化

- 启用数据缓存减少重复获取
- 使用相对换手率策略提高计算效率
- 批量处理时合理设置并发数
- 定期清理日志和缓存文件

## 风险提示

⚠️ **重要声明**

1. 本系统仅供学习和研究使用
2. 不构成任何投资建议
3. 股票投资有风险，入市需谨慎
4. 历史表现不代表未来收益
5. 使用前请充分理解策略逻辑

## 常见问题

### Q: 为什么有时候没有候选股票？
A: 可能是市场环境较差，所有策略都无法找到符合条件的股票。建议适当放宽配置参数。

### Q: 如何调整策略的严格程度？
A: 修改配置文件中的参数，如 `consecutive_days`、`max_cumulative_return` 等。

### Q: 评分结果如何解读？
A: 综合评分越高表示股票越符合策略要求，但不代表投资建议。

### Q: 系统支持哪些数据源？
A: 目前主要支持 Baostock，未来可扩展其他数据源。

## 技术支持

如有问题请查看日志文件或联系开发团队。

## 开源协议

本项目采用 MIT 开源协议。