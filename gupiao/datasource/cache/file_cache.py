import os
import hashlib
from typing import Optional, List, Dict, Any
import pandas as pd
from .storage.parquet_storage import ParquetStorage
from .range_calculator import DateRange, RangeCalculator


class FileCache:
    """文件缓存管理器"""

    def __init__(self, cache_dir: str, compression: str = 'snappy'):
        """
        初始化文件缓存管理器

        Args:
            cache_dir: 缓存根目录
            compression: 压缩算法
        """
        self.cache_dir = cache_dir
        self.storage = ParquetStorage(compression=compression)

        # 创建目录结构
        self.data_dir = os.path.join(cache_dir, 'data')
        self.by_stock_dir = os.path.join(self.data_dir, 'by_stock')
        self.by_date_dir = os.path.join(self.data_dir, 'by_date')

        for directory in [self.data_dir, self.by_stock_dir, self.by_date_dir]:
            os.makedirs(directory, exist_ok=True)

    def save_stock_data(
        self,
        stock_code: str,
        data_type: str,
        data: pd.DataFrame,
        date_range: DateRange
    ) -> str:
        """
        保存股票数据（按股票分组存储）

        Args:
            stock_code: 股票代码
            data_type: 数据类型（如 k_data_d_3, financial_data等）
            data: 数据DataFrame
            date_range: 数据日期范围

        Returns:
            str: 保存的文件路径
        """
        # 构造文件路径
        stock_dir = os.path.join(self.by_stock_dir, stock_code, data_type)
        os.makedirs(stock_dir, exist_ok=True)

        # 按年份分割文件名
        year_ranges = RangeCalculator.split_range_by_year(date_range.start, date_range.end)

        saved_files = []
        for year_range in year_ranges:
            # 过滤该年份的数据
            year_data = self._filter_data_by_date_range(data, year_range)
            if year_data.empty:
                continue

            # 构造文件名
            start_year = year_range.start[:4]
            if year_range.start[:4] == year_range.end[:4]:
                filename = f"{start_year}.parquet"
            else:
                end_year = year_range.end[:4]
                filename = f"{start_year}_{end_year}.parquet"

            file_path = os.path.join(stock_dir, filename)

            # 如果文件已存在，合并数据
            if self.storage.exists(file_path):
                existing_data = self.storage.load(file_path)
                if existing_data is not None:
                    # 合并并去重
                    combined_data = pd.concat([existing_data, year_data], ignore_index=True)
                    if 'date' in combined_data.columns:
                        combined_data = combined_data.drop_duplicates(subset=['date'], keep='last')
                        combined_data = combined_data.sort_values('date')
                    year_data = combined_data

            # 保存数据
            if self.storage.save(file_path, year_data):
                saved_files.append(file_path)

        return saved_files[0] if saved_files else ""

    def load_stock_data(
        self,
        stock_code: str,
        data_type: str,
        date_range: DateRange,
        fields: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        加载股票数据

        Args:
            stock_code: 股票代码
            data_type: 数据类型
            date_range: 日期范围
            fields: 要加载的字段列表，None表示加载所有字段

        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败时返回None
        """
        stock_dir = os.path.join(self.by_stock_dir, stock_code, data_type)
        if not os.path.exists(stock_dir):
            return None

        # 获取所有相关文件
        relevant_files = self._get_relevant_files(stock_dir, date_range)
        if not relevant_files:
            return None

        # 加载并合并数据
        all_data = []
        for file_path in relevant_files:
            if fields:
                file_data = self.storage.load_columns(file_path, fields)
            else:
                file_data = self.storage.load(file_path)

            if file_data is not None and not file_data.empty:
                all_data.append(file_data)

        if not all_data:
            return None

        # 合并所有数据
        combined_data = pd.concat(all_data, ignore_index=True)

        # 按日期范围过滤
        if 'date' in combined_data.columns:
            combined_data = self._filter_data_by_date_range(combined_data, date_range)

        return combined_data if not combined_data.empty else None

    def save_cross_sectional_data(
        self,
        date: str,
        data_type: str,
        data: pd.DataFrame
    ) -> str:
        """
        保存横截面数据（按日期分组存储）

        Args:
            date: 日期
            data_type: 数据类型（如 market_data, technical_indicators等）
            data: 数据DataFrame

        Returns:
            str: 保存的文件路径
        """
        # 构造文件路径
        date_dir = os.path.join(self.by_date_dir, data_type)
        os.makedirs(date_dir, exist_ok=True)

        filename = f"{date}.parquet"
        file_path = os.path.join(date_dir, filename)

        # 保存数据
        if self.storage.save(file_path, data):
            return file_path
        return ""

    def load_cross_sectional_data(
        self,
        date: str,
        data_type: str,
        fields: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        加载横截面数据

        Args:
            date: 日期
            data_type: 数据类型
            fields: 要加载的字段列表

        Returns:
            Optional[pd.DataFrame]: 加载的数据
        """
        date_dir = os.path.join(self.by_date_dir, data_type)
        filename = f"{date}.parquet"
        file_path = os.path.join(date_dir, filename)

        if not self.storage.exists(file_path):
            return None

        if fields:
            return self.storage.load_columns(file_path, fields)
        else:
            return self.storage.load(file_path)

    def delete_stock_data(self, stock_code: str, data_type: str) -> bool:
        """
        删除股票数据

        Args:
            stock_code: 股票代码
            data_type: 数据类型

        Returns:
            bool: 删除是否成功
        """
        stock_dir = os.path.join(self.by_stock_dir, stock_code, data_type)
        if not os.path.exists(stock_dir):
            return True

        try:
            # 删除目录下所有文件
            for filename in os.listdir(stock_dir):
                file_path = os.path.join(stock_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)

            # 删除目录
            os.rmdir(stock_dir)

            # 如果股票目录为空，也删除
            stock_parent_dir = os.path.join(self.by_stock_dir, stock_code)
            if os.path.exists(stock_parent_dir) and not os.listdir(stock_parent_dir):
                os.rmdir(stock_parent_dir)

            return True

        except Exception as e:
            print(f"删除股票数据失败 {stock_code}/{data_type}: {e}")
            return False

    def delete_cross_sectional_data(self, date: str, data_type: str) -> bool:
        """
        删除横截面数据

        Args:
            date: 日期
            data_type: 数据类型

        Returns:
            bool: 删除是否成功
        """
        date_dir = os.path.join(self.by_date_dir, data_type)
        filename = f"{date}.parquet"
        file_path = os.path.join(date_dir, filename)

        return self.storage.delete(file_path)

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        info = {
            'cache_dir': self.cache_dir,
            'total_size_bytes': 0,
            'total_files': 0,
            'by_stock': {
                'stocks_count': 0,
                'data_types': set(),
                'size_bytes': 0,
                'files_count': 0
            },
            'by_date': {
                'data_types': set(),
                'size_bytes': 0,
                'files_count': 0,
                'date_range': {'start': None, 'end': None}
            }
        }

        # 统计按股票分组的数据
        if os.path.exists(self.by_stock_dir):
            stock_codes = [d for d in os.listdir(self.by_stock_dir)
                          if os.path.isdir(os.path.join(self.by_stock_dir, d))]
            info['by_stock']['stocks_count'] = len(stock_codes)

            for stock_code in stock_codes:
                stock_dir = os.path.join(self.by_stock_dir, stock_code)
                data_types = [d for d in os.listdir(stock_dir)
                             if os.path.isdir(os.path.join(stock_dir, d))]

                for data_type in data_types:
                    info['by_stock']['data_types'].add(data_type)
                    type_dir = os.path.join(stock_dir, data_type)

                    for filename in os.listdir(type_dir):
                        file_path = os.path.join(type_dir, filename)
                        if os.path.isfile(file_path):
                            file_size = self.storage.get_file_size(file_path)
                            info['by_stock']['size_bytes'] += file_size
                            info['by_stock']['files_count'] += 1

        # 统计按日期分组的数据
        if os.path.exists(self.by_date_dir):
            data_types = [d for d in os.listdir(self.by_date_dir)
                         if os.path.isdir(os.path.join(self.by_date_dir, d))]

            all_dates = []
            for data_type in data_types:
                info['by_date']['data_types'].add(data_type)
                type_dir = os.path.join(self.by_date_dir, data_type)

                for filename in os.listdir(type_dir):
                    if filename.endswith('.parquet'):
                        file_path = os.path.join(type_dir, filename)
                        if os.path.isfile(file_path):
                            file_size = self.storage.get_file_size(file_path)
                            info['by_date']['size_bytes'] += file_size
                            info['by_date']['files_count'] += 1

                            # 提取日期
                            date = filename.replace('.parquet', '')
                            all_dates.append(date)

            if all_dates:
                all_dates.sort()
                info['by_date']['date_range'] = {
                    'start': all_dates[0],
                    'end': all_dates[-1]
                }

        # 转换集合为列表
        info['by_stock']['data_types'] = list(info['by_stock']['data_types'])
        info['by_date']['data_types'] = list(info['by_date']['data_types'])

        # 计算总计
        info['total_size_bytes'] = info['by_stock']['size_bytes'] + info['by_date']['size_bytes']
        info['total_files'] = info['by_stock']['files_count'] + info['by_date']['files_count']

        return info

    def cleanup_old_files(self, retention_days: int = 365) -> Dict[str, int]:
        """
        清理旧文件

        Args:
            retention_days: 保留天数

        Returns:
            Dict[str, int]: 清理统计信息
        """
        from datetime import datetime, timedelta

        cutoff_time = datetime.now() - timedelta(days=retention_days)
        stats = {
            'deleted_files': 0,
            'freed_bytes': 0
        }

        def cleanup_directory(directory: str):
            if not os.path.exists(directory):
                return

            for root, dirs, files in os.walk(directory):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    try:
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff_time:
                            file_size = self.storage.get_file_size(file_path)
                            if self.storage.delete(file_path):
                                stats['deleted_files'] += 1
                                stats['freed_bytes'] += file_size
                    except Exception as e:
                        print(f"清理文件失败 {file_path}: {e}")

        # 清理两个目录
        cleanup_directory(self.by_stock_dir)
        cleanup_directory(self.by_date_dir)

        return stats

    def optimize_storage(self) -> Dict[str, int]:
        """
        优化存储结构

        Returns:
            Dict[str, int]: 优化统计信息
        """
        stats = {
            'merged_files': 0,
            'freed_bytes': 0
        }

        # 这里可以实现文件合并、重新压缩等优化逻辑
        # 目前只是一个占位符实现

        return stats

    def _get_relevant_files(self, directory: str, date_range: DateRange) -> List[str]:
        """
        获取与日期范围相关的文件

        Args:
            directory: 目录路径
            date_range: 日期范围

        Returns:
            List[str]: 相关文件路径列表
        """
        if not os.path.exists(directory):
            return []

        relevant_files = []
        start_year = int(date_range.start[:4])
        end_year = int(date_range.end[:4])

        for filename in os.listdir(directory):
            if not filename.endswith('.parquet'):
                continue

            file_path = os.path.join(directory, filename)
            if not os.path.isfile(file_path):
                continue

            # 解析文件名中的年份
            base_name = filename.replace('.parquet', '')
            try:
                if '_' in base_name:
                    # 跨年文件格式：2020_2021.parquet
                    parts = base_name.split('_')
                    file_start_year = int(parts[0])
                    file_end_year = int(parts[1])
                else:
                    # 单年文件格式：2020.parquet
                    file_start_year = file_end_year = int(base_name)

                # 检查年份范围是否重叠
                if not (end_year < file_start_year or start_year > file_end_year):
                    relevant_files.append(file_path)

            except ValueError:
                # 文件名格式不匹配，跳过
                continue

        return sorted(relevant_files)

    def _filter_data_by_date_range(self, data: pd.DataFrame, date_range: DateRange) -> pd.DataFrame:
        """
        按日期范围过滤数据

        Args:
            data: 数据DataFrame
            date_range: 日期范围

        Returns:
            pd.DataFrame: 过滤后的数据
        """
        if data.empty or 'date' not in data.columns:
            return data

        # 确保date列是字符串格式
        data = data.copy()
        data['date'] = data['date'].astype(str)

        # 过滤日期范围
        mask = (data['date'] >= date_range.start) & (data['date'] <= date_range.end)
        return data[mask]

    def _generate_query_key(self, **kwargs) -> str:
        """
        生成查询键

        Args:
            **kwargs: 查询参数

        Returns:
            str: 查询键
        """
        # 将参数转换为字符串并排序
        key_parts = []
        for key, value in sorted(kwargs.items()):
            if isinstance(value, list):
                value = ','.join(map(str, value))
            key_parts.append(f"{key}={value}")

        query_string = '&'.join(key_parts)

        # 生成哈希值
        return hashlib.md5(query_string.encode('utf-8')).hexdigest()[:16]