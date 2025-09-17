import akshare as ak
import pandas as pd

from ds.StockDataSource import StockDataSource


class AkShareDataSource(StockDataSource):
    def get_stock_list(self):
        if not self.is_available():
            print("[SKIP] AkShare 冷却中，跳过请求")
            return pd.DataFrame()

        try:
            df = ak.stock_info_a_code_name()
            self.record_success()
            return df
        except Exception as e:
            self.record_failure(str(e))   # 失败时记录
            # 这里可以加额外逻辑，比如：检测是否 429 限流错误
            return pd.DataFrame()

    def get_daily(self, code, start_date, end_date=None, adjust="qfq"):
        if not self.is_available():
            return pd.DataFrame()
        try:
            df = ak.stock_zh_a_hist(symbol=code, start_date=start_date, end_date=end_date, adjust=adjust)
            self.record_success()
            return df
        except Exception as e:
            self.record_failure(str(e))
            return pd.DataFrame()

    def get_today(self, code):
        if not self.is_available():
            return pd.DataFrame()
        try:
            df = ak.stock_zh_a_spot_em()
            self.record_success()
            return df[df["代码"] == code]
        except Exception as e:
            self.record_failure(str(e))
            return pd.DataFrame()