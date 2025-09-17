"""
股票数据源抽象基类

统一的股票数据访问接口，定义所有股票数据操作
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Union, Any
from datetime import datetime
import pandas as pd


class StockDataResult:
    """股票数据结果封装类"""

    def __init__(self, success: bool = True, data: Any = None, error_code: str = "0",
                 error_msg: str = "", fields: List[str] = None):
        self.success = success
        self.data = data
        self.error_code = error_code
        self.error_msg = error_msg
        self.fields = fields or []

    def to_dataframe(self) -> pd.DataFrame:
        """将结果转换为DataFrame"""
        if not self.success or not self.data:
            return pd.DataFrame()

        if isinstance(self.data, list) and self.fields:
            return pd.DataFrame(self.data, columns=self.fields)
        elif isinstance(self.data, pd.DataFrame):
            return self.data
        else:
            return pd.DataFrame([self.data])

    def __bool__(self) -> bool:
        """支持布尔判断"""
        return self.success


class StockDataSource(ABC):
    """股票数据源抽象基类

    定义了所有股票数据操作的统一接口，包括：
    - K线数据查询
    - 财务数据查询
    - 证券信息查询
    - 宏观经济数据查询
    - 板块数据查询

    注意：登录登出由各个数据源自行管理，不作为抽象方法要求
    """

    # ==================== K线数据 ====================

    @abstractmethod
    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "d",
        adjustflag: str = "3"
    ) -> StockDataResult:
        """
        获取历史A股K线数据

        Args:
            code: 股票代码，sh或sz.+6位数字代码，如：sh.601398
            fields: 指示简称，支持多指标输入，以半角逗号分隔
            start_date: 开始日期，格式"YYYY-MM-DD"
            end_date: 结束日期，格式"YYYY-MM-DD"
            frequency: 数据类型，d=日k线、w=周、m=月、5=5分钟等
            adjustflag: 复权类型，1=后复权、2=前复权、3=不复权

        Returns:
            StockDataResult: K线数据结果
        """
        pass

    @abstractmethod
    def query_dividend_data(
        self,
        code: str,
        year: str,
        yearType: str = "trade"
    ) -> StockDataResult:
        """
        查询除权除息信息

        Args:
            code: 股票代码
            year: 年份
            yearType: 年份类别，report=报告期、trade=交易日

        Returns:
            StockDataResult: 除权除息数据结果
        """
        pass

    @abstractmethod
    def query_adjust_factor(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """
        查询复权因子信息

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            StockDataResult: 复权因子数据结果
        """
        pass

    # ==================== 财务数据 ====================

    @abstractmethod
    def query_profit_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频盈利能力数据"""
        pass

    @abstractmethod
    def query_operation_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频营运能力数据"""
        pass

    @abstractmethod
    def query_growth_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频成长能力数据"""
        pass

    @abstractmethod
    def query_balance_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频偿债能力数据"""
        pass

    @abstractmethod
    def query_cash_flow_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频现金流量数据"""
        pass

    @abstractmethod
    def query_dupont_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频杜邦指数数据"""
        pass

    @abstractmethod
    def query_performance_express_report(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询季频公司业绩快报数据"""
        pass

    @abstractmethod
    def query_forecast_report(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询季频公司业绩预告数据"""
        pass

    # ==================== 证券信息 ====================

    @abstractmethod
    def query_trade_dates(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询交易日信息"""
        pass

    @abstractmethod
    def query_all_stock(
        self,
        day: Optional[str] = None
    ) -> StockDataResult:
        """查询所有股票代码"""
        pass

    @abstractmethod
    def query_stock_basic(
        self,
        code: str,
        fields: Optional[str] = None
    ) -> StockDataResult:
        """查询证券基本资料"""
        pass

    # ==================== 宏观经济数据 ====================

    @abstractmethod
    def query_deposit_rate_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询存款利率数据"""
        pass

    @abstractmethod
    def query_loan_rate_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询贷款利率数据"""
        pass

    @abstractmethod
    def query_required_reserve_ratio_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询存款准备金率数据"""
        pass

    @abstractmethod
    def query_money_supply_data_month(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询月度货币供应量数据"""
        pass

    @abstractmethod
    def query_money_supply_data_year(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询年度货币供应量数据"""
        pass

    @abstractmethod
    def query_shibor_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询银行间同业拆放利率数据"""
        pass

    # ==================== 板块数据 ====================

    @abstractmethod
    def query_stock_industry(
        self,
        code: str,
        date: str
    ) -> StockDataResult:
        """查询行业分类数据"""
        pass

    @abstractmethod
    def query_sz50_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询上证50成分股数据"""
        pass

    @abstractmethod
    def query_hs300_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询沪深300成分股数据"""
        pass

    @abstractmethod
    def query_zz500_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询中证500成分股数据"""
        pass

    # ==================== 参数验证工具方法 ====================

    def _validate_code(self, code: str) -> bool:
        """验证股票代码格式"""
        if not code:
            return False

        # 检查格式：sh.xxxxxx 或 sz.xxxxxx
        if not (code.startswith('sh.') or code.startswith('sz.')):
            return False

        # 检查代码长度
        parts = code.split('.')
        if len(parts) != 2 or len(parts[1]) != 6:
            return False

        # 检查是否为数字
        return parts[1].isdigit()

    def _validate_date(self, date_str: str) -> bool:
        """验证日期格式 YYYY-MM-DD"""
        if not date_str:
            return True  # 空日期通常是可选的

        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def _validate_year(self, year: Any) -> bool:
        """验证年份"""
        if isinstance(year, str):
            return year.isdigit() and len(year) == 4
        elif isinstance(year, int):
            return 1990 <= year <= 2030
        return False

    def _validate_quarter(self, quarter: Any) -> bool:
        """验证季度"""
        if quarter is None:
            return True  # 季度可以为空

        if isinstance(quarter, str):
            return quarter in ['1', '2', '3', '4']
        elif isinstance(quarter, int):
            return quarter in [1, 2, 3, 4]
        return False

    def _validate_month_date(self, date_str: str) -> bool:
        """验证月份日期格式 YYYY-MM"""
        if not date_str:
            return True

        try:
            datetime.strptime(date_str, '%Y-%m')
            return True
        except ValueError:
            return False

    def _validate_year_date(self, date_str: str) -> bool:
        """验证年份日期格式 YYYY"""
        if not date_str:
            return True

        return date_str.isdigit() and len(date_str) == 4 and 1990 <= int(date_str) <= 2030