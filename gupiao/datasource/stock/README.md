# 股票数据API层

基于baostock文档定义的通用股票数据API接口层，提供统一的数据访问接口。

## 架构设计

### 核心组件

1. **base.py** - 基础抽象类和结果封装
   - `StockDataAPI`: 数据源API抽象基类
   - `BaseDataAPI`: 功能模块API基类
   - `APIResult`: API调用结果封装类

2. **功能模块**
   - `kline.py` - K线数据API
   - `financial.py` - 财务数据API
   - `security.py` - 证券信息API
   - `macro.py` - 宏观经济数据API
   - `sector.py` - 板块数据API

## API功能覆盖

### K线数据API (KLineAPI)
- `query_history_k_data_plus()` - 获取历史A股K线数据
- `query_dividend_data()` - 查询除权除息信息
- `query_adjust_factor()` - 查询复权因子信息

### 财务数据API (FinancialAPI)
- `query_profit_data()` - 季频盈利能力数据
- `query_operation_data()` - 季频营运能力数据
- `query_growth_data()` - 季频成长能力数据
- `query_balance_data()` - 季频偿债能力数据
- `query_cash_flow_data()` - 季频现金流量数据
- `query_dupont_data()` - 季频杜邦指数数据
- `query_performance_express_report()` - 业绩快报数据
- `query_forecast_report()` - 业绩预告数据

### 证券信息API (SecurityAPI)
- `query_trade_dates()` - 查询交易日信息
- `query_all_stock()` - 查询所有股票代码
- `query_stock_basic()` - 查询证券基本资料

### 宏观经济数据API (MacroAPI)
- `query_deposit_rate_data()` - 存款利率数据
- `query_loan_rate_data()` - 贷款利率数据
- `query_required_reserve_ratio_data()` - 存款准备金率数据
- `query_money_supply_data_month()` - 月度货币供应量数据
- `query_money_supply_data_year()` - 年度货币供应量数据
- `query_shibor_data()` - 银行间同业拆放利率数据

### 板块数据API (SectorAPI)
- `query_stock_industry()` - 行业分类数据
- `query_sz50_stocks()` - 上证50成分股数据
- `query_hs300_stocks()` - 沪深300成分股数据
- `query_zz500_stocks()` - 中证500成分股数据

## 使用方式

### 1. 实现数据源适配器

```python
from gupiao.datasource.api.base import StockDataAPI, APIResult

class BaostockAPI(StockDataAPI):
    def login(self) -> APIResult:
        # 实现具体的登录逻辑
        import baostock as bs
        lg = bs.login()
        self._is_logged_in = (lg.error_code == '0')
        return APIResult(
            success=self._is_logged_in,
            error_code=lg.error_code,
            error_msg=lg.error_msg
        )

    def logout(self) -> APIResult:
        # 实现具体的登出逻辑
        import baostock as bs
        bs.logout()
        self._is_logged_in = False
        return APIResult(success=True)
```

### 2. 使用API接口

```python
from gupiao.datasource.api import KLineAPI, FinancialAPI

# 创建数据源实例
stock_api = BaostockAPI()
stock_api.login()

# 创建功能API实例
kline_api = KLineAPI(stock_api)
financial_api = FinancialAPI(stock_api)

# 查询K线数据
result = kline_api.query_history_k_data_plus(
    code="sh.600000",
    fields="date,code,open,high,low,close,volume",
    start_date="2023-01-01",
    end_date="2023-12-31"
)

if result.success:
    df = result.to_dataframe()
    print(df.head())
else:
    print(f"查询失败: {result.error_msg}")

# 查询财务数据
profit_result = financial_api.query_profit_data(
    code="sh.600000",
    year=2023,
    quarter=4
)
```

### 3. 参数验证

所有API都包含完整的参数验证：
- 股票代码格式验证 (sh.xxxxxx, sz.xxxxxx)
- 日期格式验证 (YYYY-MM-DD, YYYY-MM, YYYY)
- 枚举参数验证 (frequency, adjustflag等)
- 登录状态检查

### 4. 错误处理

```python
result = api.some_method()
if not result.success:
    print(f"错误码: {result.error_code}")
    print(f"错误信息: {result.error_msg}")
```

## 错误码规范

- 10002xxx: K线数据API错误
- 10003xxx: 除权除息API错误
- 10004xxx: 复权因子API错误
- 10005xxx-10012xxx: 财务数据API错误
- 10013xxx-10015xxx: 证券信息API错误
- 10016xxx-10021xxx: 宏观经济数据API错误
- 10022xxx-10025xxx: 板块数据API错误

## 扩展性

该API层设计为可扩展的：
1. 可以轻松添加新的数据源实现
2. 可以添加新的功能模块API
3. 统一的接口规范便于不同数据源之间切换
4. 完整的参数验证和错误处理机制

## 运行示例

```bash
cd gupiao/datasource/api
python example.py
```