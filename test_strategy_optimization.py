#!/usr/bin/env python3
"""
选股策略优化存储方案测试验证脚本
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from ds.parquet.FileStockDataSource import StrategyOptimizedDataSource


class MockDataSource:
    """模拟数据源，用于测试"""

    def __init__(self):
        self.stock_codes = ['000001', '000002', '600000', '600001', '600002']

    def query_all_stock(self, date=None):
        """返回模拟股票列表"""
        return pd.DataFrame({
            'code': self.stock_codes,
            'name': [f'股票{i}' for i in range(len(self.stock_codes))]
        })

    def query_history_k_data_plus(self, code, fields, start_date, end_date, **kwargs):
        """返回模拟历史数据"""
        # 生成模拟的日线数据
        date_range = pd.date_range(start_date, end_date, freq='D')

        # 过滤掉周末
        date_range = [d for d in date_range if d.weekday() < 5]

        if not date_range:
            return pd.DataFrame()

        np.random.seed(hash(code) % 2**31)  # 确保同一股票数据一致

        data = []
        base_price = 10 + hash(code) % 50  # 基础价格

        for i, date in enumerate(date_range):
            price_change = np.random.normal(0, 0.02)  # 2%标准差的价格变动
            close_price = base_price * (1 + price_change) ** i

            data.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': code,
                'open': close_price * (1 + np.random.normal(0, 0.01)),
                'high': close_price * (1 + abs(np.random.normal(0, 0.02))),
                'low': close_price * (1 - abs(np.random.normal(0, 0.02))),
                'close': close_price,
                'volume': np.random.randint(1000000, 10000000),
                'amount': np.random.randint(50000000, 500000000),
                'turn': np.random.uniform(1.0, 10.0)
            })

        return pd.DataFrame(data)


class TestStrategyOptimizedDataSource(unittest.TestCase):
    """策略优化数据源测试类"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_source = MockDataSource()
        self.ds = StrategyOptimizedDataSource(
            real_source=self.mock_source,
            cache_dir=self.temp_dir,
            cache_days=1
        )

    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_directory_structure(self):
        """测试目录结构创建"""
        cache_dir = Path(self.temp_dir)

        # 检查主要目录是否创建
        expected_dirs = ['market_data', 'financial_data', 'screening_cache', 'metadata']
        for dir_name in expected_dirs:
            self.assertTrue((cache_dir / dir_name).exists(), f"{dir_name} 目录应该存在")

    def test_monthly_file_path(self):
        """测试月度文件路径生成"""
        test_date = "2024-03-15"
        expected_path = Path(self.temp_dir) / "market_data" / "2024_03.parquet"
        actual_path = self.ds._get_monthly_file_path(test_date)

        self.assertEqual(actual_path, expected_path)

    def test_market_snapshot(self):
        """测试市场快照功能"""
        test_date = "2024-03-15"

        # 第一次调用应该构建缓存
        snapshot = self.ds.get_market_snapshot(test_date)

        # 检查返回的数据
        self.assertIsInstance(snapshot, pd.DataFrame)

        if not snapshot.empty:
            # 检查必要的列是否存在
            required_cols = ['date', 'code', 'close', 'turn']
            for col in required_cols:
                self.assertIn(col, snapshot.columns, f"列 {col} 应该存在")

            # 检查技术指标是否计算
            indicator_cols = ['MA5', 'MA10', 'MA20', 'consecutive_up_days']
            for col in indicator_cols:
                self.assertIn(col, snapshot.columns, f"技术指标 {col} 应该存在")

    def test_daily_screening(self):
        """测试每日筛选功能"""
        test_date = "2024-03-15"
        strategy_config = {
            'max_price': 100,
            'min_consecutive_up_days': 1,  # 降低要求以便有结果
            'min_turnover': 0.5,
            'max_turnover': 15.0
        }

        # 执行筛选
        candidates = self.ds.daily_stock_screening(test_date, strategy_config)

        # 检查返回结果
        self.assertIsInstance(candidates, pd.DataFrame)

        if not candidates.empty:
            # 检查筛选条件是否生效
            self.assertTrue(all(candidates['close'] <= strategy_config['max_price']))

            if 'turn' in candidates.columns:
                self.assertTrue(all(candidates['turn'] >= strategy_config['min_turnover']))
                self.assertTrue(all(candidates['turn'] <= strategy_config['max_turnover']))

    def test_cache_persistence(self):
        """测试缓存持久化"""
        test_date = "2024-03-15"
        strategy_config = {'max_price': 100, 'min_turnover': 1.0, 'max_turnover': 10.0}

        # 第一次调用
        result1 = self.ds.daily_stock_screening(test_date, strategy_config)

        # 第二次调用应该从缓存读取
        result2 = self.ds.daily_stock_screening(test_date, strategy_config)

        # 结果应该一致
        if not result1.empty and not result2.empty:
            pd.testing.assert_frame_equal(result1.reset_index(drop=True),
                                        result2.reset_index(drop=True))

    def test_technical_indicators(self):
        """测试技术指标计算"""
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': pd.date_range('2024-03-01', periods=30, freq='D'),
            'code': ['TEST'] * 30,
            'close': [10 + i * 0.1 for i in range(30)]  # 递增价格
        })

        # 计算技术指标
        result = self.ds._add_technical_indicators(test_data)

        # 检查计算结果
        self.assertIn('MA5', result.columns)
        self.assertIn('MA10', result.columns)
        self.assertIn('MA20', result.columns)
        self.assertIn('consecutive_up_days', result.columns)

        # 检查移动平均线的合理性
        ma5_last = result['MA5'].iloc[-1]
        close_last_5 = result['close'].iloc[-5:].mean()
        self.assertAlmostEqual(ma5_last, close_last_5, places=6)

    def test_cache_info(self):
        """测试缓存信息获取"""
        cache_info = self.ds.get_cache_info()

        # 检查返回的信息结构
        expected_keys = ['cache_dir', 'file_count', 'total_size_mb', 'cache_days']
        for key in expected_keys:
            self.assertIn(key, cache_info)

        # 检查数据类型
        self.assertIsInstance(cache_info['file_count'], int)
        self.assertIsInstance(cache_info['total_size_mb'], (int, float))
        self.assertIsInstance(cache_info['cache_days'], int)


class TestPerformance(unittest.TestCase):
    """性能测试类"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mock_source = MockDataSource()
        self.ds = StrategyOptimizedDataSource(
            real_source=self.mock_source,
            cache_dir=self.temp_dir,
            cache_days=1
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_repeated_access_performance(self):
        """测试重复访问的性能提升"""
        test_date = "2024-03-15"
        strategy_config = {'max_price': 100, 'min_turnover': 1.0, 'max_turnover': 10.0}

        # 第一次访问（构建缓存）
        start_time = datetime.now()
        result1 = self.ds.daily_stock_screening(test_date, strategy_config)
        first_duration = (datetime.now() - start_time).total_seconds()

        # 第二次访问（使用缓存）
        start_time = datetime.now()
        result2 = self.ds.daily_stock_screening(test_date, strategy_config)
        second_duration = (datetime.now() - start_time).total_seconds()

        print(f"第一次耗时: {first_duration:.3f}s, 第二次耗时: {second_duration:.3f}s")

        # 第二次应该明显更快（允许一些误差）
        if first_duration > 0.1:  # 只有在第一次耗时足够长时才比较
            self.assertLess(second_duration, first_duration * 0.8, "缓存应该提升性能")


def run_validation():
    """运行完整的验证测试"""
    print("=" * 60)
    print("选股策略优化存储方案 - 验证测试")
    print("=" * 60)

    # 创建测试套件
    test_suite = unittest.TestSuite()

    # 添加功能测试
    test_suite.addTest(unittest.makeSuite(TestStrategyOptimizedDataSource))

    # 添加性能测试
    test_suite.addTest(unittest.makeSuite(TestPerformance))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # 输出测试结果汇总
    print("\n" + "=" * 60)
    print("测试结果汇总:")
    print(f"总测试数: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")

    if result.failures:
        print("\n失败的测试:")
        for test, error in result.failures:
            print(f"  - {test}: {error}")

    if result.errors:
        print("\n错误的测试:")
        for test, error in result.errors:
            print(f"  - {test}: {error}")

    success_rate = (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
    print(f"\n成功率: {success_rate:.1f}%")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_validation()

    if success:
        print("\n✓ 所有测试通过! 选股策略优化存储方案验证成功。")
    else:
        print("\n✗ 部分测试失败，请检查实现。")

    exit(0 if success else 1)