"""日期范围计算工具测试"""

import unittest
from gupiao.datasource.cache.range_calculator import DateRange, RangeCalculator


class TestDateRange(unittest.TestCase):
    """DateRange测试"""

    def test_init(self):
        """测试初始化"""
        range1 = DateRange('2023-01-01', '2023-01-31')
        self.assertEqual(range1.start, '2023-01-01')
        self.assertEqual(range1.end, '2023-01-31')

    def test_str(self):
        """测试字符串表示"""
        range1 = DateRange('2023-01-01', '2023-01-31')
        self.assertEqual(str(range1), '[2023-01-01, 2023-01-31]')

    def test_contains(self):
        """测试日期包含检查"""
        range1 = DateRange('2023-01-01', '2023-01-31')

        self.assertTrue(range1.contains('2023-01-01'))
        self.assertTrue(range1.contains('2023-01-15'))
        self.assertTrue(range1.contains('2023-01-31'))
        self.assertFalse(range1.contains('2022-12-31'))
        self.assertFalse(range1.contains('2023-02-01'))

    def test_overlaps(self):
        """测试范围重叠检查"""
        range1 = DateRange('2023-01-01', '2023-01-31')
        range2 = DateRange('2023-01-15', '2023-02-15')
        range3 = DateRange('2023-02-01', '2023-02-28')
        range4 = DateRange('2022-12-01', '2022-12-31')

        self.assertTrue(range1.overlaps(range2))
        self.assertTrue(range2.overlaps(range1))
        self.assertFalse(range1.overlaps(range3))
        self.assertFalse(range1.overlaps(range4))

    def test_is_adjacent(self):
        """测试相邻范围检查"""
        range1 = DateRange('2023-01-01', '2023-01-31')
        range2 = DateRange('2023-02-01', '2023-02-28')
        range3 = DateRange('2023-01-15', '2023-02-15')
        range4 = DateRange('2023-03-01', '2023-03-31')

        self.assertTrue(range1.is_adjacent(range2))
        self.assertTrue(range2.is_adjacent(range1))
        self.assertFalse(range1.is_adjacent(range3))  # 重叠，不是相邻
        self.assertFalse(range1.is_adjacent(range4))  # 有间隔，不是相邻

    def test_days_count(self):
        """测试天数计算"""
        range1 = DateRange('2023-01-01', '2023-01-01')  # 单天
        self.assertEqual(range1.days_count(), 1)

        range2 = DateRange('2023-01-01', '2023-01-31')  # 整月
        self.assertEqual(range2.days_count(), 31)

        range3 = DateRange('2023-01-01', '2023-12-31')  # 整年
        self.assertEqual(range3.days_count(), 365)

    def test_days_count_invalid_date(self):
        """测试无效日期的天数计算"""
        range1 = DateRange('invalid-date', '2023-01-31')
        self.assertEqual(range1.days_count(), 0)


class TestRangeCalculator(unittest.TestCase):
    """RangeCalculator测试"""

    def test_calculate_missing_ranges_no_cache(self):
        """测试无缓存时的缺失范围计算"""
        target_start = '2023-01-01'
        target_end = '2023-01-31'
        cached_ranges = []

        missing = RangeCalculator.calculate_missing_ranges(
            target_start, target_end, cached_ranges
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0].start, target_start)
        self.assertEqual(missing[0].end, target_end)

    def test_calculate_missing_ranges_full_cache(self):
        """测试完全缓存时的缺失范围计算"""
        target_start = '2023-01-01'
        target_end = '2023-01-31'
        cached_ranges = [DateRange('2023-01-01', '2023-01-31')]

        missing = RangeCalculator.calculate_missing_ranges(
            target_start, target_end, cached_ranges
        )

        self.assertEqual(len(missing), 0)

    def test_calculate_missing_ranges_partial_cache(self):
        """测试部分缓存时的缺失范围计算"""
        target_start = '2023-01-01'
        target_end = '2023-01-31'
        cached_ranges = [DateRange('2023-01-10', '2023-01-20')]

        missing = RangeCalculator.calculate_missing_ranges(
            target_start, target_end, cached_ranges
        )

        self.assertEqual(len(missing), 2)
        self.assertEqual(missing[0].start, '2023-01-01')
        self.assertEqual(missing[0].end, '2023-01-09')
        self.assertEqual(missing[1].start, '2023-01-21')
        self.assertEqual(missing[1].end, '2023-01-31')

    def test_calculate_missing_ranges_multiple_cache(self):
        """测试多个缓存范围的缺失计算"""
        target_start = '2023-01-01'
        target_end = '2023-01-31'
        cached_ranges = [
            DateRange('2023-01-05', '2023-01-10'),
            DateRange('2023-01-15', '2023-01-20'),
            DateRange('2023-01-25', '2023-01-30')
        ]

        missing = RangeCalculator.calculate_missing_ranges(
            target_start, target_end, cached_ranges
        )

        self.assertEqual(len(missing), 4)
        self.assertEqual(missing[0], DateRange('2023-01-01', '2023-01-04'))
        self.assertEqual(missing[1], DateRange('2023-01-11', '2023-01-14'))
        self.assertEqual(missing[2], DateRange('2023-01-21', '2023-01-24'))
        self.assertEqual(missing[3], DateRange('2023-01-31', '2023-01-31'))

    def test_merge_ranges_empty(self):
        """测试合并空范围列表"""
        ranges = []
        merged = RangeCalculator.merge_ranges(ranges)
        self.assertEqual(len(merged), 0)

    def test_merge_ranges_single(self):
        """测试合并单个范围"""
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        merged = RangeCalculator.merge_ranges(ranges)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0], ranges[0])

    def test_merge_ranges_overlapping(self):
        """测试合并重叠范围"""
        ranges = [
            DateRange('2023-01-01', '2023-01-15'),
            DateRange('2023-01-10', '2023-01-31')
        ]
        merged = RangeCalculator.merge_ranges(ranges)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].start, '2023-01-01')
        self.assertEqual(merged[0].end, '2023-01-31')

    def test_merge_ranges_adjacent(self):
        """测试合并相邻范围"""
        ranges = [
            DateRange('2023-01-01', '2023-01-15'),
            DateRange('2023-01-16', '2023-01-31')
        ]
        merged = RangeCalculator.merge_ranges(ranges)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].start, '2023-01-01')
        self.assertEqual(merged[0].end, '2023-01-31')

    def test_merge_ranges_separate(self):
        """测试合并分离范围"""
        ranges = [
            DateRange('2023-01-01', '2023-01-15'),
            DateRange('2023-02-01', '2023-02-15')
        ]
        merged = RangeCalculator.merge_ranges(ranges)

        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[0], ranges[0])
        self.assertEqual(merged[1], ranges[1])

    def test_split_range_by_year_single_year(self):
        """测试按年分割 - 单年"""
        ranges = RangeCalculator.split_range_by_year('2023-01-01', '2023-12-31')

        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start, '2023-01-01')
        self.assertEqual(ranges[0].end, '2023-12-31')

    def test_split_range_by_year_multiple_years(self):
        """测试按年分割 - 跨年"""
        ranges = RangeCalculator.split_range_by_year('2022-06-01', '2024-03-31')

        self.assertEqual(len(ranges), 3)
        self.assertEqual(ranges[0], DateRange('2022-06-01', '2022-12-31'))
        self.assertEqual(ranges[1], DateRange('2023-01-01', '2023-12-31'))
        self.assertEqual(ranges[2], DateRange('2024-01-01', '2024-03-31'))

    def test_split_range_by_days(self):
        """测试按天数分割"""
        ranges = RangeCalculator.split_range_by_days('2023-01-01', '2023-01-31', chunk_days=10)

        self.assertEqual(len(ranges), 4)
        self.assertEqual(ranges[0], DateRange('2023-01-01', '2023-01-10'))
        self.assertEqual(ranges[1], DateRange('2023-01-11', '2023-01-20'))
        self.assertEqual(ranges[2], DateRange('2023-01-21', '2023-01-30'))
        self.assertEqual(ranges[3], DateRange('2023-01-31', '2023-01-31'))

    def test_get_date_boundaries_empty(self):
        """测试获取空范围列表的边界"""
        ranges = []
        start, end = RangeCalculator.get_date_boundaries(ranges)
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_get_date_boundaries_single(self):
        """测试获取单个范围的边界"""
        ranges = [DateRange('2023-01-01', '2023-01-31')]
        start, end = RangeCalculator.get_date_boundaries(ranges)
        self.assertEqual(start, '2023-01-01')
        self.assertEqual(end, '2023-01-31')

    def test_get_date_boundaries_multiple(self):
        """测试获取多个范围的边界"""
        ranges = [
            DateRange('2023-02-01', '2023-02-28'),
            DateRange('2023-01-01', '2023-01-31'),
            DateRange('2023-03-01', '2023-03-15')
        ]
        start, end = RangeCalculator.get_date_boundaries(ranges)
        self.assertEqual(start, '2023-01-01')
        self.assertEqual(end, '2023-03-15')

    def test_filter_ranges_by_period(self):
        """测试按时间段过滤范围"""
        ranges = [
            DateRange('2023-01-01', '2023-01-31'),
            DateRange('2023-02-01', '2023-02-28'),
            DateRange('2023-03-01', '2023-03-31')
        ]

        filtered = RangeCalculator.filter_ranges_by_period(
            ranges, '2023-01-15', '2023-02-15'
        )

        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0], DateRange('2023-01-15', '2023-01-31'))
        self.assertEqual(filtered[1], DateRange('2023-02-01', '2023-02-15'))

    def test_calculate_coverage_ratio_empty(self):
        """测试空缓存的覆盖率"""
        target_range = DateRange('2023-01-01', '2023-01-31')
        cached_ranges = []

        ratio = RangeCalculator.calculate_coverage_ratio(target_range, cached_ranges)
        self.assertEqual(ratio, 0.0)

    def test_calculate_coverage_ratio_full(self):
        """测试完全覆盖的覆盖率"""
        target_range = DateRange('2023-01-01', '2023-01-31')
        cached_ranges = [DateRange('2023-01-01', '2023-01-31')]

        ratio = RangeCalculator.calculate_coverage_ratio(target_range, cached_ranges)
        self.assertEqual(ratio, 1.0)

    def test_calculate_coverage_ratio_partial(self):
        """测试部分覆盖的覆盖率"""
        target_range = DateRange('2023-01-01', '2023-01-31')  # 31天
        cached_ranges = [DateRange('2023-01-01', '2023-01-15')]  # 15天

        ratio = RangeCalculator.calculate_coverage_ratio(target_range, cached_ranges)
        expected_ratio = 15 / 31
        self.assertAlmostEqual(ratio, expected_ratio, places=2)

    def test_is_trading_date(self):
        """测试交易日判断"""
        # 2023-01-02是周一，应该是交易日
        self.assertTrue(RangeCalculator.is_trading_date('2023-01-02'))

        # 2023-01-07是周六，不是交易日
        self.assertFalse(RangeCalculator.is_trading_date('2023-01-07'))

        # 2023-01-08是周日，不是交易日
        self.assertFalse(RangeCalculator.is_trading_date('2023-01-08'))

        # 无效日期
        self.assertFalse(RangeCalculator.is_trading_date('invalid-date'))

    def test_get_trading_dates_in_range(self):
        """测试获取范围内交易日"""
        # 2023-01-02到2023-01-06：周一到周五，应该有5个交易日
        trading_dates = RangeCalculator.get_trading_dates_in_range('2023-01-02', '2023-01-06')
        self.assertEqual(len(trading_dates), 5)

        # 2023-01-07到2023-01-08：周六到周日，应该没有交易日
        trading_dates = RangeCalculator.get_trading_dates_in_range('2023-01-07', '2023-01-08')
        self.assertEqual(len(trading_dates), 0)

        # 无效日期范围
        trading_dates = RangeCalculator.get_trading_dates_in_range('invalid', 'invalid')
        self.assertEqual(len(trading_dates), 0)

    def test_next_date(self):
        """测试获取下一天日期"""
        next_date = RangeCalculator._next_date('2023-01-01')
        self.assertEqual(next_date, '2023-01-02')

        # 月末
        next_date = RangeCalculator._next_date('2023-01-31')
        self.assertEqual(next_date, '2023-02-01')

        # 年末
        next_date = RangeCalculator._next_date('2023-12-31')
        self.assertEqual(next_date, '2024-01-01')

        # 无效日期
        next_date = RangeCalculator._next_date('invalid-date')
        self.assertEqual(next_date, 'invalid-date')

    def test_previous_date(self):
        """测试获取前一天日期"""
        prev_date = RangeCalculator._previous_date('2023-01-02')
        self.assertEqual(prev_date, '2023-01-01')

        # 月初
        prev_date = RangeCalculator._previous_date('2023-02-01')
        self.assertEqual(prev_date, '2023-01-31')

        # 年初
        prev_date = RangeCalculator._previous_date('2023-01-01')
        self.assertEqual(prev_date, '2022-12-31')

        # 无效日期
        prev_date = RangeCalculator._previous_date('invalid-date')
        self.assertEqual(prev_date, 'invalid-date')


if __name__ == '__main__':
    unittest.main()