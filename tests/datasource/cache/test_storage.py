"""存储后端测试"""

import unittest
import tempfile
import shutil
import os
import pandas as pd
from unittest.mock import Mock, patch

from gupiao.datasource.cache.storage.base_storage import BaseStorage
from gupiao.datasource.cache.storage.parquet_storage import ParquetStorage


class TestParquetStorage(unittest.TestCase):
    """Parquet存储后端测试"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.storage = ParquetStorage(compression='snappy')

        # 创建测试数据
        self.test_data = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'open': [10.0, 11.0, 12.0],
            'high': [10.5, 11.5, 12.5],
            'low': [9.5, 10.5, 11.5],
            'close': [10.2, 11.2, 12.2],
            'volume': [1000, 1100, 1200]
        })

    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_with_valid_compression(self):
        """测试有效压缩算法初始化"""
        for compression in ['snappy', 'gzip', 'lz4', 'brotli']:
            storage = ParquetStorage(compression=compression)
            self.assertEqual(storage.compression, compression)

    def test_init_with_invalid_compression(self):
        """测试无效压缩算法初始化"""
        with self.assertRaises(ValueError):
            ParquetStorage(compression='invalid')

    def test_save_and_load_success(self):
        """测试成功保存和加载数据"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 保存数据
        result = self.storage.save(file_path, self.test_data)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(file_path))

        # 加载数据
        loaded_data = self.storage.load(file_path)
        self.assertIsNotNone(loaded_data)
        pd.testing.assert_frame_equal(loaded_data, self.test_data)

    def test_save_empty_data(self):
        """测试保存空数据"""
        file_path = os.path.join(self.temp_dir, 'empty.parquet')
        empty_data = pd.DataFrame()

        result = self.storage.save(file_path, empty_data)
        self.assertFalse(result)

    def test_save_none_data(self):
        """测试保存None数据"""
        file_path = os.path.join(self.temp_dir, 'none.parquet')

        result = self.storage.save(file_path, None)
        self.assertFalse(result)

    def test_load_nonexistent_file(self):
        """测试加载不存在的文件"""
        file_path = os.path.join(self.temp_dir, 'nonexistent.parquet')

        result = self.storage.load(file_path)
        self.assertIsNone(result)

    def test_exists(self):
        """测试文件存在性检查"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 文件不存在
        self.assertFalse(self.storage.exists(file_path))

        # 保存文件后存在
        self.storage.save(file_path, self.test_data)
        self.assertTrue(self.storage.exists(file_path))

    def test_delete(self):
        """测试文件删除"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 保存文件
        self.storage.save(file_path, self.test_data)
        self.assertTrue(self.storage.exists(file_path))

        # 删除文件
        result = self.storage.delete(file_path)
        self.assertTrue(result)
        self.assertFalse(self.storage.exists(file_path))

    def test_delete_nonexistent_file(self):
        """测试删除不存在的文件"""
        file_path = os.path.join(self.temp_dir, 'nonexistent.parquet')

        result = self.storage.delete(file_path)
        self.assertTrue(result)  # 删除不存在的文件应该返回True

    def test_get_file_size(self):
        """测试获取文件大小"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 文件不存在时大小为0
        size = self.storage.get_file_size(file_path)
        self.assertEqual(size, 0)

        # 保存文件后有大小
        self.storage.save(file_path, self.test_data)
        size = self.storage.get_file_size(file_path)
        self.assertGreater(size, 0)

    def test_append(self):
        """测试追加数据"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 保存初始数据
        self.storage.save(file_path, self.test_data)

        # 追加新数据
        new_data = pd.DataFrame({
            'date': ['2023-01-04', '2023-01-05'],
            'open': [13.0, 14.0],
            'high': [13.5, 14.5],
            'low': [12.5, 13.5],
            'close': [13.2, 14.2],
            'volume': [1300, 1400]
        })

        result = self.storage.append(file_path, new_data)
        self.assertTrue(result)

        # 验证合并后的数据
        loaded_data = self.storage.load(file_path)
        self.assertEqual(len(loaded_data), 5)

    def test_get_row_count(self):
        """测试获取行数"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 文件不存在时行数为0
        count = self.storage.get_row_count(file_path)
        self.assertEqual(count, 0)

        # 保存文件后获取行数
        self.storage.save(file_path, self.test_data)
        count = self.storage.get_row_count(file_path)
        self.assertEqual(count, 3)

    def test_get_columns(self):
        """测试获取列名"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')

        # 文件不存在时返回空列表
        columns = self.storage.get_columns(file_path)
        self.assertEqual(columns, [])

        # 保存文件后获取列名
        self.storage.save(file_path, self.test_data)
        columns = self.storage.get_columns(file_path)
        self.assertEqual(columns, ['date', 'open', 'high', 'low', 'close', 'volume'])

    def test_load_columns(self):
        """测试加载指定列"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')
        self.storage.save(file_path, self.test_data)

        # 加载指定列
        columns_to_load = ['date', 'close']
        loaded_data = self.storage.load_columns(file_path, columns_to_load)

        self.assertIsNotNone(loaded_data)
        self.assertEqual(list(loaded_data.columns), columns_to_load)
        self.assertEqual(len(loaded_data), 3)

    def test_load_columns_invalid(self):
        """测试加载无效列"""
        file_path = os.path.join(self.temp_dir, 'test.parquet')
        self.storage.save(file_path, self.test_data)

        # 加载不存在的列
        invalid_columns = ['nonexistent_column']
        loaded_data = self.storage.load_columns(file_path, invalid_columns)

        self.assertIsNone(loaded_data)

    @patch('builtins.print')
    def test_save_error_handling(self, mock_print):
        """测试保存错误处理"""
        # 使用无效路径触发错误
        invalid_path = "/invalid/path/test.parquet"

        result = self.storage.save(invalid_path, self.test_data)
        self.assertFalse(result)
        mock_print.assert_called()

    @patch('builtins.print')
    def test_load_error_handling(self, mock_print):
        """测试加载错误处理"""
        # 创建一个无效的parquet文件
        invalid_file = os.path.join(self.temp_dir, 'invalid.parquet')
        with open(invalid_file, 'w') as f:
            f.write("invalid parquet content")

        result = self.storage.load(invalid_file)
        self.assertIsNone(result)
        mock_print.assert_called()


class TestBaseStorage(unittest.TestCase):
    """基础存储类测试"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_ensure_directory(self):
        """测试确保目录存在"""
        # 创建一个具体的存储实现用于测试
        storage = ParquetStorage()

        # 测试目录不存在时创建
        test_dir = os.path.join(self.temp_dir, 'nested', 'dir')
        test_file = os.path.join(test_dir, 'test.parquet')

        self.assertFalse(os.path.exists(test_dir))
        storage.ensure_directory(test_file)
        self.assertTrue(os.path.exists(test_dir))

    def test_get_file_info(self):
        """测试获取文件信息"""
        storage = ParquetStorage()

        # 不存在的文件
        nonexistent_file = os.path.join(self.temp_dir, 'nonexistent.parquet')
        info = storage.get_file_info(nonexistent_file)

        expected = {
            'exists': False,
            'size_bytes': 0,
            'modified_time': None
        }
        self.assertEqual(info, expected)

        # 存在的文件
        test_file = os.path.join(self.temp_dir, 'test.parquet')
        test_data = pd.DataFrame({'a': [1, 2, 3]})
        storage.save(test_file, test_data)

        info = storage.get_file_info(test_file)
        self.assertTrue(info['exists'])
        self.assertGreater(info['size_bytes'], 0)
        self.assertIsNotNone(info['modified_time'])


if __name__ == '__main__':
    unittest.main()