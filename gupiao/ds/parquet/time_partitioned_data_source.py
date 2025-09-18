import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from gupiao.ds.data_source_interface import DataSourceInterface


class TimePartitionedDataSource(DataSourceInterface):
    """时间分区数据源实现 - 优化大量数据的时间范围查询性能"""

    def __init__(self, real_source: DataSourceInterface, cache_dir: str = "cache",
                 partition_type: str = "monthly", cache_days: int = 1):
        """
        初始化时间分区数据源

        Args:
            real_source: 真实的数据源（如BaoStockDataSource）
            cache_dir: 缓存目录路径
            partition_type: 分区类型 ("monthly", "yearly", "daily")
            cache_days: 缓存有效期（天），0表示永不过期
        """
        self.real_source = real_source
        self.cache_dir = Path(cache_dir)
        self.partition_type = partition_type
        self.cache_days = cache_days

        # 创建缓存目录
        self.cache_dir.mkdir(exist_ok=True)

    def _get_partition_key(self, date_str: str) -> str:
        """根据日期生成分区键"""
        if self.partition_type == "yearly":
            return date_str[:4]  # "2024"
        elif self.partition_type == "monthly":
            return date_str[:7].replace("-", "_")  # "2024-03" -> "2024_03"
        elif self.partition_type == "daily":
            return date_str.replace("-", "_")  # "2024-03-15" -> "2024_03_15"
        else:
            raise ValueError(f"Unsupported partition_type: {self.partition_type}")

    def _get_partition_file_path(self, date_str: str) -> Path:
        """获取分区文件路径"""
        partition_key = self._get_partition_key(date_str)
        return self.cache_dir / f"partition_{partition_key}.parquet"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """检查缓存是否有效"""
        if not cache_path.exists():
            return False

        if self.cache_days == 0:  # 永不过期
            return True

        cache_age = time.time() - cache_path.stat().st_mtime
        return cache_age < (self.cache_days * 24 * 3600)

    def _get_date_range_for_partition(self, date_str: str) -> tuple:
        """获取分区对应的日期范围"""
        if self.partition_type == "yearly":
            year = date_str[:4]
            return f"{year}-01-01", f"{year}-12-31"
        elif self.partition_type == "monthly":
            year_month = date_str[:7]
            year, month = int(year_month[:4]), int(year_month[5:7])

            # 计算月末最后一天
            import calendar
            last_day = calendar.monthrange(year, month)[1]

            return f"{year_month}-01", f"{year_month}-{last_day:02d}"
        elif self.partition_type == "daily":
            return date_str, date_str
        else:
            raise ValueError(f"Unsupported partition_type: {self.partition_type}")

    def _build_partition_cache(self, date_str: str, stock_codes: Optional[List[str]] = None):
        """构建分区缓存"""
        partition_file = self._get_partition_file_path(date_str)

        if self._is_cache_valid(partition_file):
            return

        print(f"Building partition cache for {self._get_partition_key(date_str)}...")

        start_date, end_date = self._get_date_range_for_partition(date_str)

        # 获取股票列表
        if stock_codes is None:
            all_stocks = self.query_all_stock()
            stock_codes = all_stocks['code'].tolist() if not all_stocks.empty else []

        # 收集该分区的所有数据
        all_partition_data = []

        for code in stock_codes:
            try:
                stock_data = self.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,volume,amount,turn",
                    start_date,
                    end_date
                )
                if not stock_data.empty:
                    all_partition_data.append(stock_data)
            except Exception as e:
                print(f"Warning: Failed to fetch data for {code}: {e}")
                continue

        if all_partition_data:
            # 合并所有数据
            partition_data = pd.concat(all_partition_data, ignore_index=True)

            # 数据类型优化和排序
            partition_data['date'] = pd.to_datetime(partition_data['date'])
            partition_data = partition_data.sort_values(['date', 'code'])

            # 使用PyArrow优化存储
            self._save_optimized_parquet(partition_data, partition_file)

            print(f"Partition cache built: {partition_file}, {len(partition_data)} records")
        else:
            print(f"No data found for partition {self._get_partition_key(date_str)}")

    def _save_optimized_parquet(self, df: pd.DataFrame, file_path: Path):
        """以优化的格式保存Parquet文件"""
        # 使用PyArrow写入，启用压缩和优化
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            file_path,
            compression='snappy',
            row_group_size=50000,  # 控制row group大小
            write_statistics=True   # 启用统计信息，支持谓词下推
        )

    def query_partition_data(self, start_date: str, end_date: str,
                           stock_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        查询分区数据 - 核心优化方法

        Args:
            start_date: 开始日期 "YYYY-MM-DD"
            end_date: 结束日期 "YYYY-MM-DD"
            stock_codes: 股票代码列表，None表示查询所有股票

        Returns:
            查询结果DataFrame
        """
        # 确定需要读取的分区文件
        partition_files = self._get_partition_files_for_range(start_date, end_date)

        all_data = []

        for partition_file in partition_files:
            if not self._is_cache_valid(partition_file):
                # 构建缓存
                partition_date = self._extract_date_from_partition_file(partition_file)
                self._build_partition_cache(partition_date, stock_codes)

            if partition_file.exists():
                try:
                    # 使用PyArrow的谓词下推进行高效过滤
                    # 将字符串日期转换为datetime对象用于PyArrow过滤
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

                    filters = [
                        ('date', '>=', start_dt),
                        ('date', '<=', end_dt)
                    ]

                    table = pq.read_table(partition_file, filters=filters)
                    df = table.to_pandas()

                    # Filter by stock codes in pandas (more flexible than PyArrow filters)
                    if stock_codes and not df.empty:
                        df = df[df['code'].isin(stock_codes)]

                    if not df.empty:
                        all_data.append(df)

                except Exception as e:
                    print(f"Error reading partition {partition_file}: {e}")
                    continue

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def _get_partition_files_for_range(self, start_date: str, end_date: str) -> List[Path]:
        """获取日期范围内需要的分区文件列表"""
        files = []

        if self.partition_type == "monthly":
            # 按月遍历 - 使用更简洁的方法
            start_year, start_month = int(start_date[:4]), int(start_date[5:7])
            end_year, end_month = int(end_date[:4]), int(end_date[5:7])

            current_year, current_month = start_year, start_month

            while (current_year, current_month) <= (end_year, end_month):
                date_str = f"{current_year:04d}-{current_month:02d}-01"
                files.append(self._get_partition_file_path(date_str))

                # 移动到下一个月
                if current_month == 12:
                    current_year += 1
                    current_month = 1
                else:
                    current_month += 1

        elif self.partition_type == "yearly":
            # 按年遍历
            start_year = int(start_date[:4])
            end_year = int(end_date[:4])

            for year in range(start_year, end_year + 1):
                date_str = f"{year}-01-01"
                files.append(self._get_partition_file_path(date_str))

        elif self.partition_type == "daily":
            # 按日遍历
            current = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                files.append(self._get_partition_file_path(date_str))
                current += timedelta(days=1)

        return files

    def _extract_date_from_partition_file(self, partition_file: Path) -> str:
        """从分区文件路径提取日期"""
        filename = partition_file.stem
        # partition_2024_03.parquet -> 2024_03 -> 2024-03-01
        partition_key = filename.replace("partition_", "")

        if self.partition_type == "monthly":
            year, month = partition_key.split("_")
            return f"{year}-{month}-01"
        elif self.partition_type == "yearly":
            return f"{partition_key}-01-01"
        elif self.partition_type == "daily":
            parts = partition_key.split("_")
            return f"{parts[0]}-{parts[1]}-{parts[2]}"

        return partition_key

    # ================== 实现DataSourceInterface接口 ==================

    def query_all_stock(self, date=None) -> pd.DataFrame:
        """查询所有股票列表"""
        return self.real_source.query_all_stock(date)

    def query_stock_basic(self, code: str) -> pd.DataFrame:
        """查询股票基本信息"""
        return self.real_source.query_stock_basic(code)

    def query_trade_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        """查询交易日期"""
        return self.real_source.query_trade_dates(start_date, end_date)

    def query_history_k_data_plus(self, code: str, fields: str, start_date: str, end_date: str,
                                  frequency: str = "d", adjustflag: str = "2") -> pd.DataFrame:
        """
        查询历史K线数据 - 支持时间分区优化

        对于大范围查询，使用分区查询优化性能
        对于单股票小范围查询，直接使用原始数据源
        """
        # 判断是否使用分区查询
        date_diff = (datetime.strptime(end_date, "%Y-%m-%d") -
                    datetime.strptime(start_date, "%Y-%m-%d")).days

        if date_diff > 30:  # 超过30天使用分区查询
            return self.query_partition_data(start_date, end_date, [code])
        else:
            # 小范围查询直接使用原始数据源
            return self.real_source.query_history_k_data_plus(
                code, fields, start_date, end_date, frequency, adjustflag
            )

    def query_stock_industry(self, date=None) -> pd.DataFrame:
        """查询股票行业信息"""
        return self.real_source.query_stock_industry(date)

    def query_sz50_stocks(self, date=None) -> pd.DataFrame:
        """查询上证50成分股"""
        return self.real_source.query_sz50_stocks(date)

    def query_hs300_stocks(self, date=None) -> pd.DataFrame:
        """查询沪深300成分股"""
        return self.real_source.query_hs300_stocks(date)

    def query_zz500_stocks(self, date=None) -> pd.DataFrame:
        """查询中证500成分股"""
        return self.real_source.query_zz500_stocks(date)

    def query_dividend_data(self, code: str, year: str, year_type: str = "report") -> pd.DataFrame:
        """查询除权除息数据"""
        return self.real_source.query_dividend_data(code, year, year_type)

    def query_profit_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        """查询盈利能力数据"""
        return self.real_source.query_profit_data(code, year, quarter)

    def query_operation_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        """查询营运能力数据"""
        return self.real_source.query_operation_data(code, year, quarter)

    def query_growth_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        """查询成长能力数据"""
        return self.real_source.query_growth_data(code, year, quarter)

    def query_balance_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        """查询偿债能力数据"""
        return self.real_source.query_balance_data(code, year, quarter)

    def query_cash_flow_data(self, code: str, year: str, quarter: str) -> pd.DataFrame:
        """查询现金流量数据"""
        return self.real_source.query_cash_flow_data(code, year, quarter)

    # ================== 缓存管理方法 ==================

    def clear_partition_cache(self, partition_key: Optional[str] = None):
        """
        清理分区缓存

        Args:
            partition_key: 分区键，如"2024_03"，None表示清理所有缓存
        """
        if partition_key:
            cache_file = self.cache_dir / f"partition_{partition_key}.parquet"
            if cache_file.exists():
                cache_file.unlink()
                print(f"Cleared cache: {cache_file}")
        else:
            for cache_file in self.cache_dir.glob("partition_*.parquet"):
                cache_file.unlink()
                print(f"Cleared cache: {cache_file}")

    def get_cache_info(self) -> dict:
        """获取缓存统计信息"""
        cache_files = list(self.cache_dir.glob("partition_*.parquet"))
        total_size = sum(f.stat().st_size for f in cache_files)

        partitions = []
        for f in cache_files:
            partition_key = f.stem.replace("partition_", "")
            partitions.append({
                "partition": partition_key,
                "size_mb": round(f.stat().st_size / 1024 / 1024, 2),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            })

        return {
            "cache_dir": str(self.cache_dir),
            "partition_type": self.partition_type,
            "partition_count": len(cache_files),
            "total_size_mb": round(total_size / 1024 / 1024, 2),
            "cache_days": self.cache_days,
            "partitions": partitions
        }


if __name__ == "__main__":
    # 使用示例
    from gupiao.ds.baostock.baostock_data_source import BaoStockDataSource

    # 创建时间分区数据源
    real_source = BaoStockDataSource()
    ds = TimePartitionedDataSource(
        real_source=real_source,
        cache_dir="cache_partitioned",
        partition_type="monthly",  # 按月分区
        cache_days=1
    )

    print("缓存信息：", ds.get_cache_info())

    # 测试时间范围查询（会触发分区构建和优化查询）
    print("\n测试时间范围查询...")
    try:
        # 查询多只股票的3个月数据 - 使用分区优化
        result = ds.query_partition_data(
            start_date="2023-12-01",
            end_date="2024-03-31",
            stock_codes=["sh.600000", "sz.000001", "sz.000002"]
        )
        print(f"查询结果: {len(result)} 条记录")

        if not result.empty:
            print("数据样例:")
            print(result[['date', 'code', 'close']].head())

    except Exception as e:
        print(f"查询失败: {e}")

    print(f"\n最终缓存信息：", ds.get_cache_info())