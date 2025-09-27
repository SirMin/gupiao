"""元数据管理器测试"""

import unittest
import tempfile
import shutil
import os
import json
import time
from unittest.mock import patch, Mock

from gupiao.datasource.cache.metadata_manager import MetadataManager
from gupiao.datasource.cache.range_calculator import DateRange


class TestMetadataManager(unittest.TestCase):
    """MetadataManager测试"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = MetadataManager(self.temp_dir, backup_interval=10)

    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init(self):
        """测试初始化"""
        self.assertEqual(self.manager.cache_dir, self.temp_dir)
        self.assertEqual(self.manager.backup_interval, 10)
        self.assertTrue(os.path.exists(self.temp_dir))

    def test_get_cached_ranges_empty(self):
        """测试获取空缓存范围"""
        ranges = self.manager.get_cached_ranges('nonexistent_key')
        self.assertEqual(len(ranges), 0)

    def test_update_cached_ranges_new_key(self):
        """测试更新新查询键的缓存范围"""
        query_key = 'test_key'
        new_ranges = [DateRange('2023-01-01', '2023-01-31')]
        fields = ['date', 'close']

        self.manager.update_cached_ranges(query_key, new_ranges, fields)

        # 验证范围被正确保存
        cached_ranges = self.manager.get_cached_ranges(query_key)
        self.assertEqual(len(cached_ranges), 1)
        self.assertEqual(cached_ranges[0].start, '2023-01-01')
        self.assertEqual(cached_ranges[0].end, '2023-01-31')

        # 验证元数据信息
        info = self.manager.get_query_info(query_key)
        self.assertIsNotNone(info)
        self.assertEqual(info['fields'], fields)
        self.assertIn('last_updated', info)

    def test_update_cached_ranges_merge(self):
        """测试范围合并更新"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 第一次更新
        ranges1 = [DateRange('2023-01-01', '2023-01-15')]
        self.manager.update_cached_ranges(query_key, ranges1, fields)

        # 第二次更新，相邻范围
        ranges2 = [DateRange('2023-01-16', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges2, fields)

        # 验证范围被合并
        cached_ranges = self.manager.get_cached_ranges(query_key)
        self.assertEqual(len(cached_ranges), 1)
        self.assertEqual(cached_ranges[0].start, '2023-01-01')
        self.assertEqual(cached_ranges[0].end, '2023-01-31')

    def test_remove_cached_range_full(self):
        """测试移除完整范围"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加范围
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)

        # 移除完整范围
        self.manager.remove_cached_range(query_key, DateRange('2023-01-01', '2023-01-31'))

        # 验证查询键被删除
        self.assertIsNone(self.manager.get_query_info(query_key))

    def test_remove_cached_range_partial(self):
        """测试移除部分范围"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加范围
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)

        # 移除中间部分
        self.manager.remove_cached_range(query_key, DateRange('2023-01-10', '2023-01-20'))

        # 验证剩余范围
        cached_ranges = self.manager.get_cached_ranges(query_key)
        self.assertEqual(len(cached_ranges), 2)
        self.assertEqual(cached_ranges[0], DateRange('2023-01-01', '2023-01-09'))
        self.assertEqual(cached_ranges[1], DateRange('2023-01-21', '2023-01-31'))

    def test_clear_query_key(self):
        """测试清除查询键"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加数据
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)
        self.assertIsNotNone(self.manager.get_query_info(query_key))

        # 清除查询键
        self.manager.clear_query_key(query_key)
        self.assertIsNone(self.manager.get_query_info(query_key))

    def test_get_all_query_keys(self):
        """测试获取所有查询键"""
        keys = ['key1', 'key2', 'key3']
        fields = ['date', 'close']

        for key in keys:
            ranges = [DateRange('2023-01-01', '2023-01-31')]
            self.manager.update_cached_ranges(key, ranges, fields)

        all_keys = self.manager.get_all_query_keys()
        self.assertEqual(set(all_keys), set(keys))

    def test_get_statistics(self):
        """测试获取统计信息"""
        # 添加一些测试数据
        query_keys = ['key1', 'key2']
        fields = ['date', 'close']

        for key in query_keys:
            ranges = [
                DateRange('2023-01-01', '2023-01-15'),
                DateRange('2023-02-01', '2023-02-15')
            ]
            self.manager.update_cached_ranges(key, ranges, fields)

        stats = self.manager.get_statistics()

        self.assertEqual(stats['total_query_keys'], 2)
        self.assertEqual(stats['total_cached_ranges'], 4)  # 2 keys * 2 ranges each
        self.assertGreater(stats['total_records'], 0)
        self.assertIsNotNone(stats['date_coverage']['start'])
        self.assertIsNotNone(stats['date_coverage']['end'])

    def test_cleanup_expired_metadata(self):
        """测试清理过期元数据"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加数据
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)

        # 清理（保留天数为0，应该清理所有数据）
        cleaned_count = self.manager.cleanup_expired_metadata(retention_days=0)

        self.assertEqual(cleaned_count, 1)
        self.assertEqual(len(self.manager.get_all_query_keys()), 0)

    def test_validate_metadata_valid(self):
        """测试验证有效元数据"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加有效数据
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)

        issues = self.manager.validate_metadata()
        self.assertEqual(len(issues), 0)

    def test_validate_metadata_invalid(self):
        """测试验证无效元数据"""
        # 直接向元数据中添加无效数据
        self.manager._metadata['invalid_key'] = {
            'cached_ranges': [
                {'start': '2023-01-31', 'end': '2023-01-01'}  # 开始日期晚于结束日期
            ],
            'last_updated': 'invalid_date',
            'fields': ['date']
        }

        issues = self.manager.validate_metadata()
        self.assertGreater(len(issues), 0)

    def test_repair_metadata(self):
        """测试修复元数据"""
        # 添加一些需要修复的数据
        self.manager._metadata['valid_key'] = {
            'cached_ranges': [
                {'start': '2023-01-01', 'end': '2023-01-15'},
                {'start': '2023-01-10', 'end': '2023-01-20'}  # 重叠范围
            ],
            'last_updated': '2023-01-01T00:00:00',
            'fields': ['date']
        }

        self.manager._metadata['invalid_key'] = {
            'cached_ranges': [
                {'start': 'invalid', 'end': 'invalid'}  # 无效日期
            ]
        }

        stats = self.manager.repair_metadata()

        self.assertGreater(stats['removed_invalid_keys'], 0)
        self.assertGreater(stats['merged_overlapping_ranges'], 0)

    def test_save_and_load_metadata(self):
        """测试保存和加载元数据"""
        query_key = 'test_key'
        fields = ['date', 'close']

        # 添加数据
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges(query_key, ranges, fields)

        # 保存
        success = self.manager.save_metadata()
        self.assertTrue(success)
        self.assertTrue(os.path.exists(self.manager.metadata_file))

        # 创建新管理器实例加载数据
        new_manager = MetadataManager(self.temp_dir)
        loaded_ranges = new_manager.get_cached_ranges(query_key)

        self.assertEqual(len(loaded_ranges), 1)
        self.assertEqual(loaded_ranges[0].start, '2023-01-01')
        self.assertEqual(loaded_ranges[0].end, '2023-01-31')

    def test_backup_recovery(self):
        """测试备份恢复"""
        # 创建主元数据文件
        metadata_data = {
            'test_key': {
                'cached_ranges': [{'start': '2023-01-01', 'end': '2023-01-31'}],
                'last_updated': '2023-01-01T00:00:00',
                'fields': ['date', 'close'],
                'total_records': 31
            }
        }

        # 只创建备份文件，不创建主文件
        with open(self.manager.backup_file, 'w', encoding='utf-8') as f:
            json.dump(metadata_data, f)

        # 创建新管理器实例，应该从备份恢复
        with patch('builtins.print') as mock_print:
            new_manager = MetadataManager(self.temp_dir)
            mock_print.assert_called_with("主元数据文件不存在，从备份恢复...")

        # 验证数据被正确加载
        ranges = new_manager.get_cached_ranges('test_key')
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start, '2023-01-01')

    def test_thread_safety(self):
        """测试线程安全"""
        import threading

        query_key = 'test_key'
        fields = ['date', 'close']
        results = []

        def update_ranges(start_day):
            try:
                ranges = [DateRange(f'2023-01-{start_day:02d}', f'2023-01-{start_day+5:02d}')]
                self.manager.update_cached_ranges(f'{query_key}_{start_day}', ranges, fields)
                results.append(True)
            except Exception:
                results.append(False)

        # 创建多个线程同时更新
        threads = []
        for i in range(1, 11):
            thread = threading.Thread(target=update_ranges, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有操作都成功
        self.assertTrue(all(results))
        self.assertEqual(len(self.manager.get_all_query_keys()), 10)

    def test_automatic_backup(self):
        """测试自动备份"""
        query_key_base = 'test_key'
        fields = ['date', 'close']

        # 进行多次操作触发自动备份
        for i in range(15):  # 超过backup_interval=10
            ranges = [DateRange('2023-01-01', '2023-01-31')]
            self.manager.update_cached_ranges(f'{query_key_base}_{i}', ranges, fields)

        # 验证元数据文件存在（说明触发了备份）
        self.assertTrue(os.path.exists(self.manager.metadata_file))

    @patch('builtins.print')
    def test_load_metadata_error_handling(self, mock_print):
        """测试加载元数据的错误处理"""
        # 创建损坏的元数据文件
        with open(self.manager.metadata_file, 'w') as f:
            f.write("invalid json content")

        # 创建新管理器实例
        new_manager = MetadataManager(self.temp_dir)

        # 验证错误被正确处理
        mock_print.assert_called()
        self.assertEqual(len(new_manager._metadata), 0)

    def test_calculate_total_records(self):
        """测试记录总数计算"""
        query_key = 'test_key'
        ranges = [
            DateRange('2023-01-02', '2023-01-06'),  # 周一到周五，5个交易日
            DateRange('2023-01-09', '2023-01-13')   # 周一到周五，5个交易日
        ]

        total_records = self.manager._calculate_total_records(query_key, ranges)

        # 验证计算结果（应该是交易日数量）
        self.assertEqual(total_records, 10)

    def test_get_metadata_file_size(self):
        """测试获取元数据文件大小"""
        # 文件不存在时
        size = self.manager._get_metadata_file_size()
        self.assertEqual(size, 0)

        # 保存一些数据后
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        self.manager.update_cached_ranges('test_key', ranges, ['date', 'close'])
        self.manager.save_metadata()

        size = self.manager._get_metadata_file_size()
        self.assertGreater(size, 0)

    def test_get_last_backup_time(self):
        """测试获取最后备份时间"""
        # 备份文件不存在时
        backup_time = self.manager._get_last_backup_time()
        self.assertIsNone(backup_time)

        # 创建备份文件后
        with open(self.manager.backup_file, 'w') as f:
            f.write('{}')

        backup_time = self.manager._get_last_backup_time()
        self.assertIsNotNone(backup_time)


if __name__ == '__main__':
    unittest.main()