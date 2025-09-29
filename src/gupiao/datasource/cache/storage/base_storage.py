from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
import os


class BaseStorage(ABC):
    """存储后端抽象基类"""

    def __init__(self, compression: str = 'snappy'):
        """
        初始化存储后端

        Args:
            compression: 压缩算法，支持 snappy, gzip, lz4
        """
        self.compression = compression

    @abstractmethod
    def save(self, file_path: str, data: pd.DataFrame) -> bool:
        """
        保存数据到文件

        Args:
            file_path: 文件路径
            data: 要保存的DataFrame数据

        Returns:
            bool: 保存是否成功
        """
        pass

    @abstractmethod
    def load(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        从文件加载数据

        Args:
            file_path: 文件路径

        Returns:
            Optional[pd.DataFrame]: 加载的数据，失败时返回None
        """
        pass

    @abstractmethod
    def exists(self, file_path: str) -> bool:
        """
        检查文件是否存在

        Args:
            file_path: 文件路径

        Returns:
            bool: 文件是否存在
        """
        pass

    @abstractmethod
    def delete(self, file_path: str) -> bool:
        """
        删除文件

        Args:
            file_path: 文件路径

        Returns:
            bool: 删除是否成功
        """
        pass

    @abstractmethod
    def get_file_size(self, file_path: str) -> int:
        """
        获取文件大小

        Args:
            file_path: 文件路径

        Returns:
            int: 文件大小（字节），文件不存在时返回0
        """
        pass

    def ensure_directory(self, file_path: str) -> None:
        """
        确保文件所在目录存在

        Args:
            file_path: 文件路径
        """
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def get_file_info(self, file_path: str) -> dict:
        """
        获取文件基本信息

        Args:
            file_path: 文件路径

        Returns:
            dict: 文件信息字典，包含 exists, size_bytes, modified_time
        """
        if not self.exists(file_path):
            return {
                'exists': False,
                'size_bytes': 0,
                'modified_time': None
            }

        stat = os.stat(file_path)
        return {
            'exists': True,
            'size_bytes': stat.st_size,
            'modified_time': stat.st_mtime
        }