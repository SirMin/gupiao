import time
from functools import wraps

import baostock as bs
import pandas as pd

from gupiao.ds.data_source_interface import DataSourceInterface


# ========== 装饰器 ==========
# ----------------- 装饰器（模块级） -----------------
def fail_safe(method):
    """
    装饰器：给实例方法添加失败计数与熔断（cooldown）功能。
    注意：method 是未绑定的函数；wrapper 在运行时以 self 作为第一个参数。
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        method_name = method.__name__

        # 确保实例有存储结构
        if not hasattr(self, "_fail_count"):
            self._fail_count = {}
        if not hasattr(self, "_cooldown_until"):
            self._cooldown_until = {}

        # 如果当前方法在冷却期，直接抛错（调用方可捕获并切换数据源）
        cooldown_until = self._cooldown_until.get(method_name, 0)
        now = time.time()
        if now < cooldown_until:
            raise RuntimeError(
                f"[COOLDOWN] {self.__class__.__name__}.{method_name} 在冷却中，直到 {time.ctime(cooldown_until)}")

        try:
            # 执行真实方法
            result = method(self, *args, **kwargs)
            # 如果执行成功：重置失败计数与冷却（视为恢复）
            self._fail_count[method_name] = 0
            # 可选: 清除冷却（若有的话）
            if method_name in self._cooldown_until:
                self._cooldown_until.pop(method_name, None)
            return result

        except Exception as e:
            # 增加失败计数
            cnt = self._fail_count.get(method_name, 0) + 1
            self._fail_count[method_name] = cnt

            # 获取阈值和冷却时间（实例可自定义属性）
            fail_threshold = getattr(self, "FAIL_THRESHOLD", 3)
            cooldown = getattr(self, "COOLDOWN", 60)

            if cnt >= fail_threshold:
                self._cooldown_until[method_name] = time.time() + cooldown
                # 打印/记录信息
                print(
                    f"[COOLDOWN] {self.__class__.__name__}.{method_name} 连续失败 {cnt} 次，触发冷却 {cooldown}s。最后异常: {e!r}")

            # 继续抛出异常，外部工厂可以捕获并切换数据源
            raise

    return wrapper


# ================= BaoStock 实现 =================
class BaoStockDataSource(DataSourceInterface):
    """Baostock 数据源实现"""
    FAIL_THRESHOLD = 3  # 连续失败阈值（可按实例/类覆盖）
    COOLDOWN = 60  # 熔断冷却时间（秒）

    def __init__(self):
        """
        初始化BaoStock数据源，登录到BaoStock服务
        如果登录失败，会抛出异常
        """
        self._fail_count = {}
        self._cooldown_until = {}
        self.session = bs.login()
        if self.session.error_code != '0':
            raise Exception(f"baostock 登录失败: {self.session.error_msg}")

    def __del__(self):
        """
        析构函数，登出BaoStock服务
        """
        try:
            bs.logout()
        except Exception:
            pass

    # ========== 内部工具 ==========
    @staticmethod
    def _to_df(rs):
        """
        将BaoStock返回的结果集转换为pandas DataFrame

        Args:
            rs: BaoStock查询返回的结果集

        Returns:
            pd.DataFrame: 包含查询结果的DataFrame

        Raises:
            Exception: 当查询结果错误时抛出异常
        """
        if rs.error_code != "0":
            raise Exception(f"baostock error: {rs.error_code}, {rs.error_msg}")
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        return pd.DataFrame(data_list, columns=rs.fields)

    # ========== 实现接口 ==========
    @fail_safe
    def query_all_stock(self, date=None):
        """
        查询指定日期的所有股票信息

        Args:
            date (str, optional): 查询日期，格式为'YYYY-MM-DD'，默认为None（当天）

        Returns:
            pd.DataFrame: 包含所有股票信息的DataFrame
        """
        return self._to_df(bs.query_all_stock(day=date))

    @fail_safe
    def query_stock_basic(self, code: str):
        """
        查询股票基本信息

        Args:
            code (str): 股票代码，例如："sh.600000"

        Returns:
            pd.DataFrame: 包含股票基本信息的DataFrame
        """
        return self._to_df(bs.query_stock_basic(code=code))

    @fail_safe
    def query_trade_dates(self, start_date: str, end_date: str):
        """
        查询指定日期范围内的交易日信息

        Args:
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str): 结束日期，格式为'YYYY-MM-DD'

        Returns:
            pd.DataFrame: 包含交易日信息的DataFrame
        """
        return self._to_df(bs.query_trade_dates(start_date=start_date, end_date=end_date))

    @fail_safe
    def query_history_k_data_plus(self, code: str, fields: str,
                                  start_date: str, end_date: str,
                                  frequency: str = "d",
                                  adjustflag: str = "2"):
        """
        查询历史K线数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            fields (str): 指定要获取的字段，用逗号分隔
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str): 结束日期，格式为'YYYY-MM-DD'
            frequency (str, optional): 数据类型，默认为"d"（日K线）
                - d=日K线
                - w=周K线
                - m=月K线
            adjustflag (str, optional): 复权类型，默认为"2"（后复权）
                - 0=不复权
                - 1=前复权
                - 2=后复权

        Returns:
            pd.DataFrame: 包含历史K线数据的DataFrame
        """
        return self._to_df(bs.query_history_k_data_plus(
            code, fields, start_date, end_date, frequency, adjustflag
        ))

    @fail_safe
    def query_stock_industry(self, date=None):
        """
        查询股票行业分类

        Args:
            date (str, optional): 查询日期，格式为'YYYY-MM-DD'，默认为None（当天）

        Returns:
            pd.DataFrame: 包含股票行业分类的DataFrame
        """
        return self._to_df(bs.query_stock_industry(date=date))

    @fail_safe
    def query_sz50_stocks(self, date=None):
        """
        查询上证50成分股

        Args:
            date (str, optional): 查询日期，格式为'YYYY-MM-DD'，默认为None（当天）

        Returns:
            pd.DataFrame: 包含上证50成分股的DataFrame
        """
        return self._to_df(bs.query_sz50_stocks(date=date))

    @fail_safe
    def query_hs300_stocks(self, date=None):
        """
        查询沪深300成分股

        Args:
            date (str, optional): 查询日期，格式为'YYYY-MM-DD'，默认为None（当天）

        Returns:
            pd.DataFrame: 包含沪深300成分股的DataFrame
        """
        return self._to_df(bs.query_hs300_stocks(date=date))

    @fail_safe
    def query_zz500_stocks(self, date=None):
        """
        查询中证500成分股

        Args:
            date (str, optional): 查询日期，格式为'YYYY-MM-DD'，默认为None（当天）

        Returns:
            pd.DataFrame: 包含中证500成分股的DataFrame
        """
        return self._to_df(bs.query_zz500_stocks(date=date))

    @fail_safe
    def query_dividend_data(self, code: str, year: str, yearType: str = "report"):
        """
        查询股票分红数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            yearType (str, optional): 年份类型，默认为"report"（报告期）
                - report=报告期
                - operate=除权除息日

        Returns:
            pd.DataFrame: 包含股票分红数据的DataFrame
        """
        return self._to_df(bs.query_dividend_data(code=code, year=year, yearType=yearType))

    @fail_safe
    def query_profit_data(self, code: str, year: str, quarter: str):
        """
        查询股票盈利能力数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            quarter (str): 查询季度，例如："1"
                - 1=一季度
                - 2=二季度
                - 3=三季度
                - 4=四季度

        Returns:
            pd.DataFrame: 包含股票盈利能力数据的DataFrame
        """
        return self._to_df(bs.query_profit_data(code=code, year=year, quarter=quarter))

    @fail_safe
    def query_operation_data(self, code: str, year: str, quarter: str):
        """
        查询股票营运能力数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            quarter (str): 查询季度，例如："1"
                - 1=一季度
                - 2=二季度
                - 3=三季度
                - 4=四季度

        Returns:
            pd.DataFrame: 包含股票营运能力数据的DataFrame
        """
        return self._to_df(bs.query_operation_data(code=code, year=year, quarter=quarter))

    @fail_safe
    def query_growth_data(self, code: str, year: str, quarter: str):
        """
        查询股票成长能力数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            quarter (str): 查询季度，例如："1"
                - 1=一季度
                - 2=二季度
                - 3=三季度
                - 4=四季度

        Returns:
            pd.DataFrame: 包含股票成长能力数据的DataFrame
        """
        return self._to_df(bs.query_growth_data(code=code, year=year, quarter=quarter))

    @fail_safe
    def query_balance_data(self, code: str, year: str, quarter: str):
        """
        查询股票偿债能力数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            quarter (str): 查询季度，例如："1"
                - 1=一季度
                - 2=二季度
                - 3=三季度
                - 4=四季度

        Returns:
            pd.DataFrame: 包含股票偿债能力数据的DataFrame
        """
        return self._to_df(bs.query_balance_data(code=code, year=year, quarter=quarter))

    @fail_safe
    def query_cash_flow_data(self, code: str, year: str, quarter: str):
        """
        查询股票现金流量数据

        Args:
            code (str): 股票代码，例如："sh.600000"
            year (str): 查询年份，例如："2025"
            quarter (str): 查询季度，例如："1"
                - 1=一季度
                - 2=二季度
                - 3=三季度
                - 4=四季度

        Returns:
            pd.DataFrame: 包含股票现金流量数据的DataFrame
        """
        return self._to_df(bs.query_cash_flow_data(code=code, year=year, quarter=quarter))


if __name__ == "__main__":
    ds = BaoStockDataSource()

    # 股票列表
    stocks = ds.query_all_stock("2025-09-16")
    print("股票列表：", stocks.head())

    # 历史行情
    df = ds.query_history_k_data_plus(
        "sh.600000",
        "date,code,open,high,low,close,volume,amount,turn",
        "2025-01-01",
        "2025-09-01"
    )
    print("历史行情：", df.tail())

    # 交易日历
    trade_days = ds.query_trade_dates("2025-01-01", "2025-02-01")
    print("交易日历：", trade_days.head())
