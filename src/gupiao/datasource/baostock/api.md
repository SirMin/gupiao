# Baostock API 文档

## 1 入门示例
### 1.1 HelloWorld

```python
import baostock as bs
import pandas as pd

# 登陆系统
lg = bs.login()
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 获取沪深A股历史K线数据
rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date='2017-12-01', end_date='2017-12-31',
    frequency="d", adjustflag="3")
print('query_history_k_data_plus respond error_code:' + rs.error_code)
print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

# 打印结果集
data_list = []
while (rs.error_code == '0') & rs.next():
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

# 登出系统
bs.logout()
```

## 2 登录
### 2.1 login()

**功能说明：** 登陆系统

**参数：** 无

**返回类型：** LoginResult

**返回参数：**
- error_code：错误代码，'0'表示成功
- error_msg：错误信息
- user_id：用户ID
- user_name：用户名

**示例：**
```python
import baostock as bs
lg = bs.login()
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)
```

## 3 登出
### 3.1 logout()

**功能说明：** 登出系统

**参数：** 无

**返回类型：** LogoutResult

**返回参数：**
- error_code：错误代码，'0'表示成功
- error_msg：错误信息

**示例：**
```python
bs.logout()
```

## 4 获取历史A股K线数据
### 4.1 获取历史A股K线数据：query_history_k_data_plus()

**功能说明：** 获取A股K线数据，可以通过参数设置获取日k、周k、月k数据，也可以获取复权或者不复权数据。

**参数：**
- code：股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳
- fields：指示简称，支持多指标输入，以半角逗号分隔，填写内容如下：
  - date：日期
  - code：证券代码
  - open：今开盘价格
  - high：最高价
  - low：最低价
  - close：今收盘价
  - preclose：昨日收盘价
  - volume：成交量（累计 单位：股）
  - amount：成交额（单位：人民币元）
  - adjustflag：复权状态(1：后复权， 2：前复权，3：不复权）
  - turn：换手率 [注：指定交易日的成交量/指定交易日的股票的流通股总数 * 100]
  - tradestatus：交易状态(1：正常交易 0：停牌）
  - pctChg：涨跌幅（百分比）
  - isST：是否ST股，1是，0否
- start_date：开始日期（包含），格式"YYYY-MM-DD"，为空时取2015-01-01
- end_date：结束日期（包含），格式"YYYY-MM-DD"，为空时取最近一个交易日
- frequency：数据类型，默认为d，日k线；d=日k线、w=周、m=月、5=5分钟、15=15分钟、30=30分钟、60=60分钟k线数据，不区分大小写；指数无分钟线数据；周线每周最后一个交易日才可以获取，月线每月最后一个交易日才可以获取。
- adjustflag：复权类型，默认不复权：3；1：后复权；2：前复权。已支持分钟线、日线、周线、月线前后复权。

**返回类型：** ResultData

**返回字段：** 同fields参数

**示例：**
```python
rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date='2017-12-01', end_date='2017-12-31',
    frequency="d", adjustflag="3")
```

### 4.2 历史行情指标参数

| 参数名称 | 参数描述 | 算法说明 |
|----------|----------|----------|
| open | 今开盘价格 | 今日开盘价格 |
| high | 最高价 | 今日最高价格 |
| low | 最低价 | 今日最低价格 |
| close | 今收盘价 | 今日收盘价格 |
| preclose | 昨日收盘价 | 昨日收盘价格 |
| volume | 成交量 | 成交量，单位为股 |
| amount | 成交额 | 成交额，单位为人民币元 |
| turn | 换手率 | 成交量/流通股总数 * 100 |
| pctChg | 涨跌幅 | (今收盘价-昨收盘价)/昨收盘价 * 100 |

## 5 查询除权除息信息
### 5.1 除权除息信息：query_dividend_data()

**功能说明：** 查询股票除权除息信息

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：年份，如：2017
- yearType：年份类别：report报告期；trade交易日。默认为trade

**返回字段：**
- dividOperateDate：除权除息日期
- dividCashPsBeforeTax：每股分红（税前）
- dividCashPsAfterTax：每股分红（税后）
- dividStockPs：每股送股
- dividReservePs：每股转增
- dividCashStock：现金分红总额

**示例：**
```python
rs_dividend = bs.query_dividend_data(code="sh.600000", year="2018", yearType="report")
```

## 6 查询复权因子信息
### 6.1 复权因子：query_adjust_factor()

**功能说明：** 查询复权因子信息

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- dividOperateDate：除权除息日期
- foreAdjustFactor：前复权因子
- backAdjustFactor：后复权因子

**示例：**
```python
rs_factor = bs.query_adjust_factor(code="sh.600000", start_date="2018-01-01", end_date="2018-12-31")
```

## 7 查询季频财务数据信息
### 7.1 季频盈利能力：query_profit_data()

**功能说明：** 查询季频盈利能力数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度。不为空时只有4个取值：1，2，3，4

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- roeAvg：净资产收益率(平均)(%)
- npMargin：销售净利率(%)
- gpMargin：销售毛利率(%)
- netProfit：净利润(元)
- epsTTM：每股收益
- MBRevenue：主营营业收入(元)
- totalShare：总股本
- liqaShare：流通股本

**示例：**
```python
profit_list = []
rs_profit = bs.query_profit_data(code="sh.600000", year=2017, quarter=2)
```

### 7.2 季频营运能力：query_operation_data()

**功能说明：** 查询季频营运能力数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- NRTurnRatio：应收账款周转率(次)
- NRTurnDays：应收账款周转天数(天)
- INVTurnRatio：存货周转率(次)
- INVTurnDays：存货周转天数(天)
- CATurnRatio：流动资产周转率(次)
- AssetTurnRatio：总资产周转率(次)

**示例：**
```python
operation_list = []
rs_operation = bs.query_operation_data(code="sh.600000", year=2017, quarter=2)
```

### 7.3 季频成长能力：query_growth_data()

**功能说明：** 查询季频成长能力数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- YOYEquity：净资产同比增长率(%)
- YOYAsset：总资产同比增长率(%)
- YOYNI：净利润同比增长率(%)
- YOYEPSBasic：基本每股收益同比增长率(%)
- YOYROE：净资产收益率(摊薄)同比增长率(%)
- YOYPEGBasic：每股收益3年复合增长率(%)
- YOYSales：营业收入同比增长率(%)

**示例：**
```python
growth_list = []
rs_growth = bs.query_growth_data(code="sh.600000", year=2017, quarter=2)
```

### 7.4 季频偿债能力：query_balance_data()

**功能说明：** 查询季频偿债能力数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- currentRatio：流动比率
- quickRatio：速动比率
- cashRatio：现金比率
- YOYLiability：总负债同比增长率(%)
- liabilityToAsset：资产负债率(%)
- assetToEquity：权益乘数

**示例：**
```python
balance_list = []
rs_balance = bs.query_balance_data(code="sh.600000", year=2017, quarter=2)
```

### 7.5 季频现金流量：query_cash_flow_data()

**功能说明：** 查询季频现金流量数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- CAToRevenue：经营活动产生的现金流量净额/营业收入(%)
- NCAToRevenue：经营活动产生的现金流量净额/净利润(%)
- CAToAsset：经营活动产生的现金流量净额/总资产(%)
- CAToCurrentLiability：经营活动产生的现金流量净额/流动负债(%)
- CAToNetCashFlows：经营活动产生的现金流量净额/现金及现金等价物净增加额(%)

**示例：**
```python
cash_flow_list = []
rs_cash_flow = bs.query_cash_flow_data(code="sh.600000", year=2017, quarter=2)
```

### 7.6 季频杜邦指数：query_dupont_data()

**功能说明：** 查询季频杜邦指数数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- year：统计年份，如：2017
- quarter：统计季度，可为空，默认当前季度

**返回字段：**
- pubDate：公司发布财报的日期
- statDate：财报统计的季度的最后一天
- dupontROE：净资产收益率(%)
- dupontAssetStoEquity：权益乘数
- dupontAssetTurn：总资产周转率
- dupontPnitmargin：销售净利率(%)
- dupontTaxBurden：税负担比率(%)
- dupontIntburden：利息负担比率(%)
- dupontEbittoebt：税息前利润/利润总额(%)

**示例：**
```python
dupont_list = []
rs_dupont = bs.query_dupont_data(code="sh.600000", year=2017, quarter=2)
```

## 8 查询季频公司报告信息
### 8.1 季频公司业绩快报：query_performance_express_report()

**功能说明：** 查询季频公司业绩快报数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- performanceExpPubDate：业绩快报披露日期
- performanceExpStatDate：业绩快报统计日期
- performanceExpUpdateDate：业绩快报披露日期（最新）
- performanceExpressROEWa：加权平均净资产收益率(%)
- performanceExpressEPSChgPct：基本每股收益增长率(%)
- performanceExpressROEWaChgPct：加权平均净资产收益率增长率(%)
- performanceExpressEPS：每股收益
- performanceExpressNetProfitChgPct：净利润增长率(%)
- performanceExpressTotalAsset：总资产
- performanceExpressNetProfit：净利润
- performanceExpressROEDiluted：稀释净资产收益率(%)

**示例：**
```python
rs_forecast = bs.query_performance_express_report(code="sh.600000", start_date="2015-01-01", end_date="2017-12-31")
```

### 8.2 季频公司业绩预告：query_forecast_report()

**功能说明：** 查询季频公司业绩预告数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- profitForcastExpPubDate：业绩预告发布日期
- profitForcastExpStatDate：业绩预告统计日期
- profitForcastType：业绩预告类型
- profitForcastAbstract：业绩预告摘要
- profitForcastChgPctUp：预告净利润变动幅度上限(%)
- profitForcastChgPctDwn：预告净利润变动幅度下限(%)

**示例：**
```python
rs_forecast = bs.query_forecast_report(code="sh.600000", start_date="2015-01-01", end_date="2017-12-31")
```

## 9 获取证券元信息
### 9.1 交易日查询：query_trade_dates()

**功能说明：** 查询交易日信息

**参数：**
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- calendar_date：日历日期
- is_trading_day：是否交易日，1是0否

**示例：**
```python
rs = bs.query_trade_dates(start_date="2017-01-01", end_date="2017-06-30")
```

### 9.2 证券代码查询：query_all_stock()

**功能说明：** 查询所有股票代码

**参数：**
- day：查询日期，格式"YYYY-MM-DD"，为空时默认最新日期

**返回字段：**
- code：证券代码
- tradeStatus：交易状态：1正常交易 0停牌
- code_name：证券名称

**示例：**
```python
rs = bs.query_all_stock(day="2017-06-26")
```

### 9.3 证券基本资料：query_stock_basic()

**功能说明：** 查询证券基本资料

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- fields：查询字段，支持多字段查询，以半角逗号分隔。默认为空，返回全部字段，不建议返回全部字段，容易超时

可选字段：
- code：证券代码
- code_name：证券名称
- industry：所属行业
- industryClassification：行业分类
- subIndustryClassification：二级行业分类
- isGem：是否创业板：1是 0否
- isSme：是否中小企业板：1是 0否
- isHs300s：是否沪深300成分：1是 0否
- isSz50s：是否上证50成分：1是 0否
- isZz500s：是否中证500成分：1是 0否

**示例：**
```python
rs = bs.query_stock_basic(code="sh.600000")
```

## 10 宏观经济数据
### 10.1 存款利率：query_deposit_rate_data()

**功能说明：** 查询存款利率数据

**参数：**
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- publishDate：发布日期
- effectiveDate：生效日期
- depositType：存款类型
- rate：利率(%)

**示例：**
```python
rs = bs.query_deposit_rate_data(start_date="2015-01-01", end_date="2017-12-31")
```

### 10.2 贷款利率：query_loan_rate_data()

**功能说明：** 查询贷款利率数据

**参数：**
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- publishDate：发布日期
- effectiveDate：生效日期
- loanType：贷款类型
- rate：利率(%)

**示例：**
```python
rs = bs.query_loan_rate_data(start_date="2015-01-01", end_date="2017-12-31")
```

### 10.3 存款准备金率：query_required_reserve_ratio_data()

**功能说明：** 查询存款准备金率数据

**参数：**
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- publishDate：发布日期
- effectiveDate：生效日期
- rateType：准备金率类型
- rate：利率(%)

**示例：**
```python
rs = bs.query_required_reserve_ratio_data(start_date="2015-01-01", end_date="2017-12-31")
```

### 10.4 货币供应量：query_money_supply_data_month()

**功能说明：** 查询月度货币供应量数据

**参数：**
- start_date：开始日期，格式"YYYY-MM"
- end_date：结束日期，格式"YYYY-MM"

**返回字段：**
- statDate：统计时间
- m0Month：货币和准货币(M2)期末余额(亿元)
- m0YOY：货币和准货币(M2)同比增长(%)
- m1Month：货币(M1)期末余额(亿元)
- m1YOY：货币(M1)同比增长(%)
- m2Month：流通中货币(M0)期末余额(亿元)
- m2YOY：流通中货币(M0)同比增长(%)

**示例：**
```python
rs = bs.query_money_supply_data_month(start_date="2015-01", end_date="2017-12")
```

### 10.5 货币供应量(年底余额)：query_money_supply_data_year()

**功能说明：** 查询年度货币供应量数据

**参数：**
- start_date：开始日期，格式"YYYY"
- end_date：结束日期，格式"YYYY"

**返回字段：**
- statDate：统计时间
- m0：流通中货币(M0)(亿元)
- m1：货币(M1)(亿元)
- m2：货币和准货币(M2)(亿元)

**示例：**
```python
rs = bs.query_money_supply_data_year(start_date="2015", end_date="2017")
```

### 10.6 银行间同业拆放利率：query_shibor_data()

**功能说明：** 查询银行间同业拆放利率数据

**参数：**
- start_date：开始日期，格式"YYYY-MM-DD"
- end_date：结束日期，格式"YYYY-MM-DD"

**返回字段：**
- pubDate：发布日期
- d1：隔夜利率(%)
- d7：1周利率(%)
- d14：2周利率(%)
- m1：1月利率(%)
- m3：3月利率(%)
- m6：6月利率(%)
- m9：9月利率(%)
- y1：1年利率(%)

**示例：**
```python
rs = bs.query_shibor_data(start_date="2015-01-01", end_date="2017-12-31")
```

## 11 板块数据
### 11.1 行业分类：query_stock_industry()

**功能说明：** 查询行业分类数据

**参数：**
- code：股票代码，sh或sz.+6位数字代码，如：sh.601398
- date：查询日期，格式"YYYY-MM-DD"

**返回字段：**
- updateDate：更新日期
- code：证券代码
- code_name：证券名称
- industry：行业
- industryClassification：行业分类

**示例：**
```python
rs = bs.query_stock_industry(code="sh.600000", date="2015-12-31")
```

### 11.2 上证50成分股：query_sz50_stocks()

**功能说明：** 查询上证50成分股数据

**参数：**
- date：查询日期，格式"YYYY-MM-DD"

**返回字段：**
- updateDate：更新日期
- code：证券代码
- code_name：证券名称

**示例：**
```python
rs = bs.query_sz50_stocks(date="2017-12-31")
```

### 11.3 沪深300成分股：query_hs300_stocks()

**功能说明：** 查询沪深300成分股数据

**参数：**
- date：查询日期，格式"YYYY-MM-DD"

**返回字段：**
- updateDate：更新日期
- code：证券代码
- code_name：证券名称

**示例：**
```python
rs = bs.query_hs300_stocks(date="2017-12-31")
```

### 11.4 中证500成分股：query_zz500_stocks()

**功能说明：** 查询中证500成分股数据

**参数：**
- date：查询日期，格式"YYYY-MM-DD"

**返回字段：**
- updateDate：更新日期
- code：证券代码
- code_name：证券名称

**示例：**
```python
rs = bs.query_zz500_stocks(date="2017-12-31")
```

## 通用数据处理模式

所有查询方法都返回ResultData对象，处理数据的通用模式如下：

```python
# 打印结果集
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)
print(result)

# 结果集输出到csv文件
result.to_csv("D:\\data.csv", encoding="gbk", index=False)
```

## 错误码说明

| 错误码 | 说明 |
|--------|------|
| 0 | 成功 |
| 10001000 | 用户未登录 |
| 10001001 | 用户登录信息有误，请重新登录 |
| 10004003 | 请求过于频繁，请稍后再试 |
| 10004004 | 超过了每日最大请求限制 |