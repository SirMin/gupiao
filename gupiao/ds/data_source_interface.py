from abc import ABC, abstractmethod
import pandas as pd


class DataSourceInterface(ABC):
    """数据源接口，定义统一方法"""

    # ========== 股票列表 ==========
    @abstractmethod
    def query_all_stock(self, date=None) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_stock_basic(self, code: str) -> pd.DataFrame:
        pass

    # ========== 交易日 ==========
    @abstractmethod
    def query_trade_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        pass

    # ========== 历史行情 ==========
    @abstractmethod
    def query_history_k_data_plus(self, code: str, fields: str,
                                  start_date: str, end_date: str,
                                  frequency: str = "d",
                                  adjustflag: str = "2") -> pd.DataFrame:
        pass

    # ========== 行业分类 ==========
    @abstractmethod
    def query_stock_industry(self, date=None) -> pd.DataFrame:
        pass

    # ========== 指数行情 ==========
    @abstractmethod
    def query_sz50_stocks(self, date=None) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_hs300_stocks(self, date=None) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_zz500_stocks(self, date=None) -> pd.DataFrame:
        pass

    # ========== 财务数据 ==========
    @abstractmethod
    def query_dividend_data(self, code: str, year: str, yearType: str = "report") -> pd.DataFrame:
        pass

    @abstractmethod
    def query_profit_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_operation_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_growth_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_balance_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def query_cash_flow_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        pass
