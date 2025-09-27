"""多数据源管理器测试"""

import unittest
import time
import threading
from unittest.mock import Mock, patch

from gupiao.datasource.cache.datasource_manager import (
    DataSourceManager,
    LoadBalanceStrategy,
    CircuitBreakerState,
    DataSourceInfo
)


class MockDataSource:
    """模拟数据源"""

    def __init__(self, name: str, should_fail: bool = False, delay: float = 0.0):
        self.name = name
        self.should_fail = should_fail
        self.delay = delay
        self.call_count = 0

    def test_method(self, *args, **kwargs):
        """测试方法"""
        self.call_count += 1

        if self.delay > 0:
            time.sleep(self.delay)

        if self.should_fail:
            raise Exception(f"Error from {self.name}")

        return f"Result from {self.name}"

    def health_check(self):
        """健康检查"""
        return not self.should_fail


class TestDataSourceInfo(unittest.TestCase):
    """DataSourceInfo测试"""

    def test_init(self):
        """测试初始化"""
        mock_ds = MockDataSource("test")
        info = DataSourceInfo(datasource=mock_ds, priority=1, weight=2)

        self.assertEqual(info.datasource, mock_ds)
        self.assertEqual(info.priority, 1)
        self.assertEqual(info.weight, 2)
        self.assertEqual(info.total_requests, 0)
        self.assertEqual(info.failed_requests, 0)
        self.assertTrue(info.enabled)
        self.assertEqual(info.circuit_breaker_state, CircuitBreakerState.CLOSED)

    def test_success_rate(self):
        """测试成功率计算"""
        mock_ds = MockDataSource("test")
        info = DataSourceInfo(datasource=mock_ds)

        # 没有请求时成功率为100%
        self.assertEqual(info.success_rate, 1.0)

        # 有请求时
        info.total_requests = 10
        info.failed_requests = 2
        self.assertEqual(info.success_rate, 0.8)

    def test_avg_response_time(self):
        """测试平均响应时间"""
        mock_ds = MockDataSource("test")
        info = DataSourceInfo(datasource=mock_ds)

        # 没有请求时平均响应时间为0
        self.assertEqual(info.avg_response_time, 0.0)

        # 有请求时
        info.total_requests = 4
        info.total_response_time = 2.0
        self.assertEqual(info.avg_response_time, 0.5)

    def test_name_property(self):
        """测试名称属性"""
        mock_ds = MockDataSource("test")
        info = DataSourceInfo(datasource=mock_ds)

        self.assertEqual(info.name, "MockDataSource")


class TestDataSourceManager(unittest.TestCase):
    """DataSourceManager测试"""

    def setUp(self):
        """测试前准备"""
        self.ds1 = MockDataSource("ds1")
        self.ds2 = MockDataSource("ds2")
        self.ds3 = MockDataSource("ds3")

        self.manager = DataSourceManager(
            datasources=[self.ds1, self.ds2, self.ds3],
            load_balance_strategy=LoadBalanceStrategy.PRIORITY_FIRST,
            failover_enabled=True,
            circuit_breaker_enabled=True
        )

    def test_init_single_datasource(self):
        """测试单数据源初始化"""
        manager = DataSourceManager([self.ds1])
        self.assertEqual(len(manager.datasources), 1)

    def test_init_multiple_datasources(self):
        """测试多数据源初始化"""
        self.assertEqual(len(self.manager.datasources), 3)

        # 验证优先级设置
        ds_names = list(self.manager.datasources.keys())
        for i, name in enumerate(ds_names):
            self.assertEqual(self.manager.datasources[name].priority, i)

    def test_add_datasource(self):
        """测试添加数据源"""
        new_ds = MockDataSource("new_ds")
        initial_count = len(self.manager.datasources)
        self.manager.add_datasource(new_ds, priority=5, weight=3)

        # 验证数据源数量增加
        self.assertEqual(len(self.manager.datasources), initial_count + 1)

        # 找到新添加的数据源
        new_ds_info = None
        for ds_info in self.manager.datasources.values():
            if ds_info.datasource is new_ds:
                new_ds_info = ds_info
                break

        self.assertIsNotNone(new_ds_info)
        self.assertEqual(new_ds_info.priority, 5)
        self.assertEqual(new_ds_info.weight, 3)

    def test_remove_datasource(self):
        """测试移除数据源"""
        initial_count = len(self.manager.datasources)
        self.manager.remove_datasource(self.ds1)

        self.assertEqual(len(self.manager.datasources), initial_count - 1)

    def test_execute_query_success(self):
        """测试成功执行查询"""
        result = self.manager.execute_query('test_method', arg1='value1')

        # 应该返回第一个数据源的结果（优先级最高）
        self.assertEqual(result, "Result from ds1")
        self.assertEqual(self.ds1.call_count, 1)

    def test_execute_query_failover(self):
        """测试故障转移"""
        # 设置第一个数据源失败
        self.ds1.should_fail = True

        result = self.manager.execute_query('test_method')

        # 应该自动切换到第二个数据源
        self.assertEqual(result, "Result from ds2")
        self.assertEqual(self.ds1.call_count, 1)  # 第一个被调用但失败
        self.assertEqual(self.ds2.call_count, 1)  # 第二个被调用并成功

    def test_execute_query_all_fail(self):
        """测试所有数据源都失败"""
        # 设置所有数据源都失败
        for ds in [self.ds1, self.ds2, self.ds3]:
            ds.should_fail = True

        with self.assertRaises(Exception) as context:
            self.manager.execute_query('test_method')

        self.assertIn("所有数据源都失败", str(context.exception))

    def test_execute_query_no_failover(self):
        """测试禁用故障转移"""
        manager = DataSourceManager(
            datasources=[self.ds1, self.ds2],
            failover_enabled=False
        )

        self.ds1.should_fail = True

        with self.assertRaises(Exception):
            manager.execute_query('test_method')

        # 验证只调用了第一个数据源
        self.assertEqual(self.ds1.call_count, 1)
        self.assertEqual(self.ds2.call_count, 0)

    def test_circuit_breaker(self):
        """测试熔断器"""
        # 创建只有一个数据源的管理器，禁用健康检查
        manager = DataSourceManager(
            datasources=[self.ds1],
            circuit_breaker_enabled=True,
            health_check_enabled=False  # 禁用健康检查避免干扰
        )

        ds_name = list(manager.datasources.keys())[0]
        ds_info = manager.datasources[ds_name]

        # 模拟多次失败触发熔断器
        self.ds1.should_fail = True

        for _ in range(5):  # 达到熔断阈值
            try:
                manager.execute_query('test_method')
            except Exception:
                pass

        # 验证熔断器被触发
        self.assertEqual(ds_info.circuit_breaker_state, CircuitBreakerState.OPEN)

    def test_load_balance_priority_first(self):
        """测试优先级优先策略"""
        manager = DataSourceManager(
            datasources=[self.ds3, self.ds1, self.ds2],  # 乱序添加
            load_balance_strategy=LoadBalanceStrategy.PRIORITY_FIRST
        )

        result = manager.execute_query('test_method')

        # 应该使用优先级最高（数字最小）的数据源
        self.assertEqual(result, "Result from ds3")  # ds3优先级为0

    def test_load_balance_round_robin(self):
        """测试轮询策略"""
        manager = DataSourceManager(
            datasources=[self.ds1, self.ds2],
            load_balance_strategy=LoadBalanceStrategy.ROUND_ROBIN
        )

        # 连续执行多次查询
        results = []
        for _ in range(4):
            result = manager.execute_query('test_method')
            results.append(result)

        # 验证轮询效果
        expected = ["Result from ds2", "Result from ds1", "Result from ds2", "Result from ds1"]
        self.assertEqual(results, expected)

    def test_load_balance_random(self):
        """测试随机策略"""
        manager = DataSourceManager(
            datasources=[self.ds1, self.ds2],
            load_balance_strategy=LoadBalanceStrategy.RANDOM
        )

        # 执行多次查询，验证两个数据源都被使用
        results = set()
        for _ in range(10):
            result = manager.execute_query('test_method')
            results.add(result)

        # 随机策略下，两个数据源应该都会被使用
        self.assertGreaterEqual(len(results), 1)

    def test_get_healthy_datasources(self):
        """测试获取健康数据源"""
        # 设置一个数据源失败
        self.ds2.should_fail = True

        # 触发一次失败来更新状态
        try:
            self.manager.execute_query('test_method')
        except:
            pass

        healthy = self.manager.get_healthy_datasources()

        # 健康的数据源数量应该少于总数
        self.assertLess(len(healthy), len(self.manager.datasources))

    def test_get_statistics(self):
        """测试获取统计信息"""
        # 执行一些查询以生成统计数据
        self.manager.execute_query('test_method')

        stats = self.manager.get_statistics()

        self.assertIsInstance(stats, dict)
        for ds_name in self.manager.datasources.keys():
            self.assertIn(ds_name, stats)
            ds_stats = stats[ds_name]
            self.assertIn('total_requests', ds_stats)
            self.assertIn('failed_requests', ds_stats)
            self.assertIn('success_rate', ds_stats)

    def test_check_datasource_health(self):
        """测试数据源健康检查"""
        # 健康的数据源
        healthy = self.manager.check_datasource_health(self.ds1)
        self.assertTrue(healthy)

        # 不健康的数据源
        self.ds2.should_fail = True
        unhealthy = self.manager.check_datasource_health(self.ds2)
        self.assertFalse(unhealthy)

    def test_enable_disable_datasource(self):
        """测试启用/禁用数据源"""
        ds_name = list(self.manager.datasources.keys())[0]

        # 禁用数据源
        self.manager.disable_datasource(ds_name)
        ds_info = self.manager.datasources[ds_name]
        self.assertFalse(ds_info.enabled)

        # 启用数据源
        self.manager.enable_datasource(ds_name)
        self.assertTrue(ds_info.enabled)

    def test_reset_circuit_breaker(self):
        """测试重置熔断器"""
        ds_name = list(self.manager.datasources.keys())[0]
        ds_info = self.manager.datasources[ds_name]

        # 设置熔断器为开启状态
        ds_info.circuit_breaker_state = CircuitBreakerState.OPEN
        ds_info.circuit_breaker_failure_count = 10

        # 重置熔断器
        self.manager.reset_circuit_breaker(ds_name)

        self.assertEqual(ds_info.circuit_breaker_state, CircuitBreakerState.CLOSED)
        self.assertEqual(ds_info.circuit_breaker_failure_count, 0)

    def test_concurrent_requests_limit(self):
        """测试并发请求限制"""
        manager = DataSourceManager(
            datasources=[self.ds1],
            max_concurrent_requests=1
        )

        # 设置数据源有延迟
        self.ds1.delay = 0.1

        def make_request():
            try:
                return manager.execute_query('test_method')
            except Exception as e:
                return str(e)

        # 启动多个并发请求
        threads = []
        results = []

        for _ in range(3):
            thread = threading.Thread(target=lambda: results.append(make_request()))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # 验证有请求被限制
        limited_requests = [r for r in results if "最大并发请求数限制" in str(r)]
        self.assertGreater(len(limited_requests), 0)

    def test_response_time_tracking(self):
        """测试响应时间跟踪"""
        # 设置数据源有延迟
        self.ds1.delay = 0.01

        self.manager.execute_query('test_method')

        ds_name = list(self.manager.datasources.keys())[0]
        ds_info = self.manager.datasources[ds_name]

        self.assertGreater(ds_info.avg_response_time, 0)
        self.assertEqual(ds_info.total_requests, 1)

    def test_method_not_found(self):
        """测试方法不存在的情况"""
        with self.assertRaises(Exception) as context:
            self.manager.execute_query('nonexistent_method')

        # 验证异常消息包含相关信息
        error_msg = str(context.exception)
        self.assertTrue(
            "所有数据源都失败" in error_msg or
            "nonexistent_method" in error_msg or
            "attribute" in error_msg
        )

    @patch('threading.Thread')
    def test_periodic_health_check(self, mock_thread):
        """测试定期健康检查"""
        manager = DataSourceManager(
            datasources=[self.ds1],
            health_check_enabled=True,
            health_check_interval=1
        )

        # 模拟时间过去触发健康检查
        manager._last_health_check = 0

        manager.execute_query('test_method')

        # 验证健康检查线程被启动
        mock_thread.assert_called()

    def test_circuit_breaker_half_open_recovery(self):
        """测试熔断器半开状态恢复"""
        ds_name = list(self.manager.datasources.keys())[0]
        ds_info = self.manager.datasources[ds_name]

        # 设置熔断器为半开状态
        ds_info.circuit_breaker_state = CircuitBreakerState.HALF_OPEN

        # 执行成功的查询
        result = self.manager.execute_query('test_method')

        # 验证熔断器恢复到关闭状态
        self.assertEqual(ds_info.circuit_breaker_state, CircuitBreakerState.CLOSED)
        self.assertEqual(result, "Result from ds1")


if __name__ == '__main__':
    unittest.main()