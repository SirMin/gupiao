from typing import Optional
import pandas as pd
import os
from .base_storage import BaseStorage


class ParquetStorage(BaseStorage):
    """Parquet格式存储后端"""

    def __init__(self, compression: str = 'snappy'):
        """
        初始化Parquet存储后端

        Args:
            compression: 压缩算法，支持 snappy, gzip, lz4, brotli
        """
        super().__init__(compression)
        # 验证压缩算法
        supported_compression = ['snappy', 'gzip', 'lz4', 'brotli', None]
        if compression not in supported_compression:
            raise ValueError(f"不支持的压缩算法: {compression}，支持的算法: {supported_compression}")

    def save(self, file_path: str, data: pd.DataFrame) -> bool:
        """
        保存DataFrame到Parquet文件

        Args:
            file_path: 文件路径
            data: 要保存的DataFrame数据

        Returns:
            bool: 保存是否成功
        """
        try:
            if data is None or data.empty:
                return False

            # 确保目录存在
            self.ensure_directory(file_path)

            # 保存到Parquet文件
            data.to_parquet(
                file_path,
                compression=self.compression,
                index=False,
                engine='pyarrow'
            )
            return True

        except Exception as e:
            print(f"保存Parquet文件失败 {file_path}: {e}")
            return False

    def load(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        从Parquet文件加载DataFrame

        Args:
            file_path: 文件路径

        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败时返回None
        """
        try:
            if not self.exists(file_path):
                return None

            # 从Parquet文件读取
            data = pd.read_parquet(file_path, engine='pyarrow')
            return data

        except Exception as e:
            print(f"加载Parquet文件失败 {file_path}: {e}")
            return None

    def exists(self, file_path: str) -> bool:
        """
        检查Parquet文件是否存在

        Args:
            file_path: 文件路径

        Returns:
            bool: 文件是否存在
        """
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def delete(self, file_path: str) -> bool:
        """
        删除Parquet文件

        Args:
            file_path: 文件路径

        Returns:
            bool: 删除是否成功
        """
        try:
            if self.exists(file_path):
                os.remove(file_path)
            return True
        except Exception as e:
            print(f"删除Parquet文件失败 {file_path}: {e}")
            return False

    def get_file_size(self, file_path: str) -> int:
        """
        获取Parquet文件大小

        Args:
            file_path: 文件路径

        Returns:
            int: 文件大小（字节），文件不存在时返回0
        """
        try:
            if self.exists(file_path):
                return os.path.getsize(file_path)
            return 0
        except Exception:
            return 0

    def append(self, file_path: str, data: pd.DataFrame) -> bool:
        """
        向Parquet文件追加数据

        Args:
            file_path: 文件路径
            data: 要追加的DataFrame数据

        Returns:
            bool: 追加是否成功
        """
        try:
            if data is None or data.empty:
                return False

            # 如果文件存在，先读取原数据
            if self.exists(file_path):
                existing_data = self.load(file_path)
                if existing_data is not None:
                    # 合并数据
                    combined_data = pd.concat([existing_data, data], ignore_index=True)
                    return self.save(file_path, combined_data)

            # 文件不存在，直接保存
            return self.save(file_path, data)

        except Exception as e:
            print(f"追加Parquet文件失败 {file_path}: {e}")
            return False

    def get_row_count(self, file_path: str) -> int:
        """
        获取Parquet文件的行数

        Args:
            file_path: 文件路径

        Returns:
            int: 行数，文件不存在或读取失败时返回0
        """
        try:
            if not self.exists(file_path):
                return 0

            # 使用pyarrow获取行数（更高效，不读取全部数据）
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            return parquet_file.metadata.num_rows

        except Exception as e:
            print(f"获取Parquet文件行数失败 {file_path}: {e}")
            return 0

    def get_columns(self, file_path: str) -> list:
        """
        获取Parquet文件的列名

        Args:
            file_path: 文件路径

        Returns:
            list: 列名列表，文件不存在或读取失败时返回空列表
        """
        try:
            if not self.exists(file_path):
                return []

            # 使用pyarrow获取列信息（更高效）
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(file_path)
            return parquet_file.schema.names

        except Exception as e:
            print(f"获取Parquet文件列信息失败 {file_path}: {e}")
            return []

    def load_columns(self, file_path: str, columns: list) -> Optional[pd.DataFrame]:
        """
        从Parquet文件加载指定列

        Args:
            file_path: 文件路径
            columns: 要加载的列名列表

        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败时返回None
        """
        try:
            if not self.exists(file_path):
                return None

            # 验证列是否存在
            available_columns = self.get_columns(file_path)
            valid_columns = [col for col in columns if col in available_columns]

            if not valid_columns:
                print(f"警告: 没有找到有效的列 {columns} 在文件 {file_path} 中")
                return None

            # 读取指定列
            data = pd.read_parquet(file_path, columns=valid_columns, engine='pyarrow')
            return data

        except Exception as e:
            print(f"加载Parquet文件指定列失败 {file_path}: {e}")
            return None