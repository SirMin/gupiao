import pandas as pd

from ds.ak.AkShareDataSource import AkShareDataSource
# from ds.baostock.BaostockDataSource import BaostockDataSource


class DataSourceFactory:
    def __init__(self, cache=None):
        sources = [
            # (BaostockDataSource(), 1),
            (AkShareDataSource(), 2),

            # (TencentDataSource(), 2),
            # (SinaDataSource(), 3),
        ]
        self.sources = sorted(sources, key=lambda x: x[1])  # (实例, 优先级)
        self.cache = cache

    def _load_or_fetch(self, key: str, fetch_func):
        if self.cache:
            df = self.cache.load(key)
            if df is not None:
                print(f"[CACHE] {key} 命中缓存")
                return df

        for src, prio in self.sources:
            if not src.is_available():
                print(f"[SKIP] {src.__class__.__name__} 在冷却期，跳过")
                continue

            df = fetch_func(src)
            if df is not None and not df.empty:
                if self.cache:
                    self.cache.save(key, df)
                    print(f"[CACHE] {key} 已写入缓存")
                return df

        return pd.DataFrame()

    def get_stock_list(self):
        return self._load_or_fetch("stock_list", lambda src: src.get_stock_list())

    def get_daily(self, code, start_date, end_date=None, adjust="qfq"):
        key = f"daily_{code}_{start_date}_{end_date}_{adjust}"
        return self._load_or_fetch(key, lambda src: src.get_daily(code, start_date, end_date, adjust))

    def get_today(self, code):
        key = f"today_{code}_{pd.Timestamp.today().strftime('%Y%m%d')}"
        return self._load_or_fetch(key, lambda src: src.get_today(code))


if __name__ == "__main__":
    ds = DataSourceFactory()
    stock_list = ds.get_stock_list()
    print(stock_list)
    for stock in stock_list.values:
        print(ds.get_daily(stock[0], "20250103",  "20250107"))

    print(ds.get_today("600000"))
