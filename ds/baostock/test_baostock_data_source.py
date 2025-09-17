import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import time

from ds.baostock.BaostockDataSource import BaoStockDataSource


class TestBaoStockDataSource(unittest.TestCase):

    def setUp(self):
        """测试前准备"""
        with patch('ds.baostock.BaostockDataSource.bs.login') as mock_login:
            mock_session = MagicMock()
            mock_session.error_code = '0'
            mock_login.return_value = mock_session
            self.datasource = BaoStockDataSource()

    def test_init_success(self):
        """测试初始化成功"""
        with patch('ds.baostock.BaostockDataSource.bs.login') as mock_login:
            mock_session = MagicMock()
            mock_session.error_code = '0'
            mock_login.return_value = mock_session

            datasource = BaoStockDataSource()
            self.assertIsNotNone(datasource)
            mock_login.assert_called_once()

    def test_init_failure(self):
        """测试初始化失败"""
        with patch('ds.baostock.BaostockDataSource.bs.login') as mock_login:
            mock_session = MagicMock()
            mock_session.error_code = '1'
            mock_session.error_msg = '登录失败'
            mock_login.return_value = mock_session

            with self.assertRaises(Exception) as context:
                BaoStockDataSource()

            self.assertIn('baostock 登录失败', str(context.exception))

    def test_to_df_success(self):
        """测试_to_df方法成功转换"""
        mock_rs = MagicMock()
        mock_rs.error_code = '0'
        mock_rs.fields = ['col1', 'col2']
        mock_rs.next.side_effect = [True, True, False]  # 模拟有两行数据
        mock_rs.get_row_data.side_effect = [['a1', 'b1'], ['a2', 'b2']]

        df = BaoStockDataSource._to_df(mock_rs)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ['col1', 'col2'])

    def test_to_df_error(self):
        """测试_to_df方法处理错误"""
        mock_rs = MagicMock()
        mock_rs.error_code = '1'
        mock_rs.error_msg = '查询错误'

        with self.assertRaises(Exception) as context:
            BaoStockDataSource._to_df(mock_rs)

        self.assertIn('baostock error', str(context.exception))

    @patch('ds.baostock.BaostockDataSource.bs.query_all_stock')
    def test_query_all_stock(self, mock_query):
        """测试query_all_stock方法"""
        # 模拟返回结果
        mock_rs = MagicMock()
        mock_rs.error_code = '0'
        mock_rs.fields = ['date', 'code', 'name']
        mock_rs.next.side_effect = [True, False]
        mock_rs.get_row_data.return_value = ['2025-09-16', 'sh.600000', '浦发银行']
        mock_query.return_value = mock_rs

        df = self.datasource.query_all_stock('2025-09-16')

        self.assertIsInstance(df, pd.DataFrame)
        mock_query.assert_called_once_with(day='2025-09-16')

    @patch('ds.baostock.BaostockDataSource.bs.query_stock_basic')
    def test_query_stock_basic(self, mock_query):
        """测试query_stock_basic方法"""
        mock_rs = MagicMock()
        mock_rs.error_code = '0'
        mock_rs.fields = ['code', 'name']
        mock_rs.next.side_effect = [True, False]
        mock_rs.get_row_data.return_value = ['sh.600000', '浦发银行']
        mock_query.return_value = mock_rs

        df = self.datasource.query_stock_basic('sh.600000')

        self.assertIsInstance(df, pd.DataFrame)
        mock_query.assert_called_once_with(code='sh.600000')

    @patch('ds.baostock.BaostockDataSource.bs.query_trade_dates')
    def test_query_trade_dates(self, mock_query):
        """测试query_trade_dates方法"""
        mock_rs = MagicMock()
        mock_rs.error_code = '0'
        mock_rs.fields = ['date', 'is_trading_day']
        mock_rs.next.side_effect = [True, False]
        mock_rs.get_row_data.return_value = ['2025-09-16', '1']
        mock_query.return_value = mock_rs

        df = self.datasource.query_trade_dates('2025-09-15', '2025-09-16')

        self.assertIsInstance(df, pd.DataFrame)
        mock_query.assert_called_once_with(start_date='2025-09-15', end_date='2025-09-16')

    def test_fail_safe_decorator_success(self):
        """测试fail_safe装饰器成功情况"""
        with patch('ds.baostock.BaostockDataSource.bs.query_all_stock') as mock_query:
            mock_rs = MagicMock()
            mock_rs.error_code = '0'
            mock_rs.fields = ['date']
            mock_rs.next.side_effect = [True, False]
            mock_rs.get_row_data.return_value = ['2025-09-16']
            mock_query.return_value = mock_rs

            # 重置计数器
            self.datasource._fail_count = {}
            self.datasource._cooldown_until = {}

            df = self.datasource.query_all_stock('2025-09-16')

            # 验证调用成功且失败计数为0
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(self.datasource._fail_count.get('query_all_stock', 0), 0)

    def test_fail_safe_decorator_failure(self):
        """测试fail_safe装饰器失败处理"""
        with patch('ds.baostock.BaostockDataSource.bs.query_all_stock') as mock_query:
            mock_query.side_effect = Exception('网络错误')

            # 重置计数器
            self.datasource._fail_count = {}
            self.datasource._cooldown_until = {}

            # 连续失败FAIL_THRESHOLD-1次
            for i in range(BaoStockDataSource.FAIL_THRESHOLD - 1):
                with self.assertRaises(Exception) as context:
                    self.datasource.query_all_stock('2025-09-16')
                self.assertEqual(str(context.exception), '网络错误')
                self.assertEqual(self.datasource._fail_count['query_all_stock'], i + 1)

            # 第FAIL_THRESHOLD次失败，应该触发熔断
            with self.assertRaises(Exception) as context:
                self.datasource.query_all_stock('2025-09-16')
            self.assertEqual(str(context.exception), '网络错误')

            # 检查是否设置了熔断
            self.assertIn('query_all_stock', self.datasource._cooldown_until)
            self.assertGreater(self.datasource._cooldown_until['query_all_stock'], time.time())

    def test_fail_safe_decorator_cool_down(self):
        """测试fail_safe装饰器熔断机制"""
        # 手动设置熔断
        self.datasource._cooldown_until = {
            'query_all_stock': time.time() + 10  # 10秒后解封
        }

        with self.assertRaises(RuntimeError) as context:
            self.datasource.query_all_stock('2025-09-16')

        self.assertIn('已熔断', str(context.exception))


if __name__ == '__main__':
    unittest.main()
