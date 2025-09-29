# Baostock 数据源实现

基于 [baostock](http://baostock.com/) 开源证券数据平台实现的股票数据源，遵循 `gupiao.datasource.stock` 模块定义的统一接口规范。

## 特性

- **完整接口实现**: 实现了所有 `StockDataSource` 抽象基类定义的方法
- **参数验证**: 内置完整的参数格式验证
- **错误处理**: 统一的错误处理和异常捕获机制
- **日志支持**: 集成日志记录功能
- **自动登出**: 析构时自动登出，防止资源泄露

## 依赖安装

```bash
pip install baostock
```

## 基本使用

### 快速开始

```python
from gupiao.datasource.baostock import BaostockDataSource

# 创建数据源实例
bs_data = BaostockDataSource()

# 登录
login_result = bs_data.login()
if login_result.success:
    print("登录成功")

    # 查询K线数据
    kline_result = bs_data.query_history_k_data_plus(
        code="sh.600000",
        fields="date,code,open,high,low,close,volume,amount",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )

    if kline_result.success:
        # 转换为DataFrame
        df = kline_result.to_dataframe()
        print(f"获取到 {len(df)} 条K线数据")
        print(df.head())

    # 登出
    bs_data.logout()
```

### 数据类型支持

#### K线数据
- `query_history_k_data_plus()` - 历史K线数据
- `query_dividend_data()` - 除权除息信息
- `query_adjust_factor()` - 复权因子数据

#### 财务数据
- `query_profit_data()` - 盈利能力数据
- `query_operation_data()` - 营运能力数据
- `query_growth_data()` - 成长能力数据
- `query_balance_data()` - 偿债能力数据
- `query_cash_flow_data()` - 现金流量数据
- `query_dupont_data()` - 杜邦指数数据
- `query_performance_express_report()` - 业绩快报
- `query_forecast_report()` - 业绩预告

#### 证券信息
- `query_trade_dates()` - 交易日信息
- `query_all_stock()` - 所有股票代码
- `query_stock_basic()` - 股票基本信息

#### 宏观经济数据
- `query_deposit_rate_data()` - 存款利率
- `query_loan_rate_data()` - 贷款利率
- `query_required_reserve_ratio_data()` - 存款准备金率
- `query_money_supply_data_month()` - 月度货币供应量
- `query_money_supply_data_year()` - 年度货币供应量
- `query_shibor_data()` - 银行间同业拆放利率

#### 板块数据
- `query_stock_industry()` - 行业分类
- `query_sz50_stocks()` - 上证50成分股
- `query_hs300_stocks()` - 沪深300成分股
- `query_zz500_stocks()` - 中证500成分股

## 详细示例

### 1. K线数据查询

```python
# 查询日K线数据
result = bs_data.query_history_k_data_plus(
    code="sh.600000",
    fields="date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
    start_date="2024-01-01",
    end_date="2024-01-31",
    frequency="d",      # d=日线, w=周线, m=月线
    adjustflag="3"      # 1=后复权, 2=前复权, 3=不复权
)

if result.success:
    df = result.to_dataframe()
    print("K线数据:")
    print(df)
```

### 2. 财务数据查询

```python
# 查询盈利能力数据
result = bs_data.query_profit_data(
    code="sh.600000",
    year=2023,
    quarter=4  # 可选，不指定则返回年度数据
)

if result.success:
    df = result.to_dataframe()
    print("盈利能力数据:")
    print(df)
```

### 3. 股票列表查询

```python
# 查询所有股票
result = bs_data.query_all_stock("2024-01-02")

if result.success:
    df = result.to_dataframe()
    print(f"共有 {len(df)} 只股票")
    print(df.head())
```

### 4. 错误处理

```python
result = bs_data.query_history_k_data_plus("invalid_code", "date,close")

if not result.success:
    print(f"查询失败: {result.error_code} - {result.error_msg}")
```

## 错误码说明

| 错误码 | 说明 |
|--------|------|
| `NOT_LOGGED_IN` | 未登录系统 |
| `INVALID_CODE` | 股票代码格式错误 |
| `INVALID_DATE` | 日期格式错误 |
| `INVALID_YEAR` | 年份格式错误 |
| `INVALID_QUARTER` | 季度参数错误 |
| `QUERY_ERROR` | 查询异常 |
| `BAOSTOCK_LOGIN_ERROR` | 登录异常 |
| `BAOSTOCK_LOGOUT_ERROR` | 登出异常 |

## 注意事项

1. **登录要求**: 使用前必须先调用 `login()` 方法
2. **股票代码格式**: 必须使用 `sh.xxxxxx` 或 `sz.xxxxxx` 格式
3. **日期格式**: 使用 `YYYY-MM-DD` 格式
4. **自动登出**: 对象销毁时会自动登出，建议显式调用 `logout()`
5. **网络依赖**: 需要网络连接访问 baostock 服务器

## 运行示例

```bash
# 在项目根目录下运行
cd /path/to/gupiao
python -m gupiao.datasource.baostock.example
```

## 性能建议

1. **批量查询**: 尽量使用较大的日期范围进行批量查询
2. **字段选择**: 只查询需要的字段以减少网络传输
3. **连接复用**: 保持登录状态进行多次查询，最后统一登出
4. **异常处理**: 做好网络异常和数据异常的处理

## 限制说明

- 遵循 baostock 平台的使用限制和频率限制
- 部分数据可能存在延迟或缺失
- 免费版本可能有数据范围限制

## 更多信息

- [baostock 官方文档](http://baostock.com/baostock/index.html)
- [API 参考文档](api.md)
- [股票数据源接口规范](../stock/README.md)