import pandas as pd
from psycopg2 import pool

from ds.DataSourceInterface import DataSourceInterface


class PostgresStockDataSource(DataSourceInterface):

    def __init__(self, real_source: DataSourceInterface,
                 minconn: int = 0, maxconn: int = 20,
                 dsn: str = "dbname=stock user=postgres password=postgres host=localhost port=5431"):
        self.real_source = real_source
        self.pg_pool = pool.SimpleConnectionPool(minconn=minconn, maxconn=maxconn, dsn=dsn)

    def query_all_stock(self, date=None) -> pd.DataFrame:
        return self.real_source.query_all_stock(date=date)

    def query_stock_basic(self, code: str) -> pd.DataFrame:
        return self.real_source.query_stock_basic(code=code)

    def query_trade_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self.real_source.query_trade_dates(start_date=start_date, end_date=end_date)

    def query_history_k_data_plus(self, code: str, fields: str, start_date: str, end_date: str, frequency: str = "d",
                                  adjustflag: str = "2") -> pd.DataFrame:
        return self.real_source.query_history_k_data_plus(code=code, fields=fields, start_date=start_date, end_date=end_date, frequency=frequency, adjustflag=adjustflag)

    def query_stock_industry(self, date=None) -> pd.DataFrame:
        return self.real_source.query_stock_industry(date=date)

    def query_sz50_stocks(self, date=None) -> pd.DataFrame:
        return self.real_source.query_sz50_stocks(date=date)

    def query_hs300_stocks(self, date=None) -> pd.DataFrame:
        return self.real_source.query_hs300_stocks(date=date)

    def query_zz500_stocks(self, date=None) -> pd.DataFrame:
        return self.real_source.query_zz500_stocks(date=date)

    def query_dividend_data(self, code: str, year: str, yearType: str = "report") -> pd.DataFrame:
        return self.real_source.query_dividend_data(code=code, year=year, yearType=yearType)

    def query_profit_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        return self.real_source.query_profit_data(code=code, year=year, quarter=quarter)

    def query_operation_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        return self.real_source.query_operation_data(code=code, year=year, quarter=quarter)

    def query_growth_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        return self.real_source.query_growth_data(code=code, year=year, quarter=quarter)

    def query_balance_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        return self.real_source.query_balance_data(code=code, year=year, quarter=quarter)

    def query_cash_flow_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        return self.real_source.query_cash_flow_data(code=code, year=year, quarter=quarter)

