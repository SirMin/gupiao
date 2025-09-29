from typing import List, Tuple, NamedTuple, Optional
from datetime import datetime, timedelta
import pandas as pd


class DateRange(NamedTuple):
    """日期范围数据结构"""
    start: str  # YYYY-MM-DD格式
    end: str    # YYYY-MM-DD格式

    def __str__(self):
        return f"[{self.start}, {self.end}]"

    def contains(self, date: str) -> bool:
        """检查日期是否在范围内"""
        return self.start <= date <= self.end

    def overlaps(self, other: 'DateRange') -> bool:
        """检查是否与另一个范围重叠"""
        return not (self.end < other.start or other.end < self.start)

    def is_adjacent(self, other: 'DateRange') -> bool:
        """检查是否与另一个范围相邻（可以合并）"""
        # 计算日期差是否为1天
        try:
            end_date = datetime.strptime(self.end, '%Y-%m-%d')
            other_start_date = datetime.strptime(other.start, '%Y-%m-%d')
            start_date = datetime.strptime(self.start, '%Y-%m-%d')
            other_end_date = datetime.strptime(other.end, '%Y-%m-%d')

            # 检查是否相邻
            return (
                (end_date + timedelta(days=1)).strftime('%Y-%m-%d') == other.start or
                (other_end_date + timedelta(days=1)).strftime('%Y-%m-%d') == self.start
            )
        except ValueError:
            return False

    def days_count(self) -> int:
        """计算范围内的天数"""
        try:
            start_date = datetime.strptime(self.start, '%Y-%m-%d')
            end_date = datetime.strptime(self.end, '%Y-%m-%d')
            return (end_date - start_date).days + 1
        except ValueError:
            return 0


class RangeCalculator:
    """日期范围计算工具类"""

    @staticmethod
    def calculate_missing_ranges(
        target_start: str,
        target_end: str,
        cached_ranges: List[DateRange]
    ) -> List[DateRange]:
        """
        计算目标范围与已缓存范围的差集

        Args:
            target_start: 目标开始日期 (YYYY-MM-DD)
            target_end: 目标结束日期 (YYYY-MM-DD)
            cached_ranges: 已缓存的日期范围列表

        Returns:
            List[DateRange]: 需要从远程获取的日期范围列表
        """
        if not cached_ranges:
            return [DateRange(target_start, target_end)]

        target_range = DateRange(target_start, target_end)

        # 过滤出与目标范围有重叠的缓存范围
        overlapping_ranges = [
            r for r in cached_ranges
            if r.overlaps(target_range)
        ]

        if not overlapping_ranges:
            return [target_range]

        # 按开始日期排序
        overlapping_ranges.sort(key=lambda x: x.start)

        missing_ranges = []
        current_pos = target_start

        for cached_range in overlapping_ranges:
            # 如果当前位置小于缓存范围开始，添加缺失范围
            if current_pos < cached_range.start:
                missing_end = RangeCalculator._previous_date(cached_range.start)
                if current_pos <= missing_end:
                    missing_ranges.append(DateRange(current_pos, missing_end))

            # 更新当前位置到缓存范围结束后
            cached_end = min(cached_range.end, target_end)
            if cached_end >= current_pos:
                current_pos = RangeCalculator._next_date(cached_end)

        # 检查最后是否还有缺失范围
        if current_pos <= target_end:
            missing_ranges.append(DateRange(current_pos, target_end))

        return missing_ranges

    @staticmethod
    def merge_ranges(ranges: List[DateRange]) -> List[DateRange]:
        """
        合并重叠或相邻的日期范围

        Args:
            ranges: 日期范围列表

        Returns:
            List[DateRange]: 合并后的日期范围列表
        """
        if not ranges:
            return []

        # 按开始日期排序
        sorted_ranges = sorted(ranges, key=lambda x: x.start)
        merged = [sorted_ranges[0]]

        for current in sorted_ranges[1:]:
            last = merged[-1]

            # 如果当前范围与最后一个范围重叠或相邻，合并它们
            if last.overlaps(current) or last.is_adjacent(current):
                merged[-1] = DateRange(
                    min(last.start, current.start),
                    max(last.end, current.end)
                )
            else:
                merged.append(current)

        return merged

    @staticmethod
    def split_range_by_year(start: str, end: str) -> List[DateRange]:
        """
        按年份分割日期范围

        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)

        Returns:
            List[DateRange]: 按年份分割的日期范围列表
        """
        try:
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
        except ValueError:
            return [DateRange(start, end)]

        ranges = []
        current_date = start_date

        while current_date <= end_date:
            # 当前年的结束日期
            year_end = datetime(current_date.year, 12, 31)
            range_end = min(year_end, end_date)

            ranges.append(DateRange(
                current_date.strftime('%Y-%m-%d'),
                range_end.strftime('%Y-%m-%d')
            ))

            # 移动到下一年的开始
            current_date = datetime(current_date.year + 1, 1, 1)

        return ranges

    @staticmethod
    def split_range_by_days(start: str, end: str, chunk_days: int = 30) -> List[DateRange]:
        """
        按天数分割日期范围

        Args:
            start: 开始日期 (YYYY-MM-DD)
            end: 结束日期 (YYYY-MM-DD)
            chunk_days: 每个分块的天数

        Returns:
            List[DateRange]: 分割后的日期范围列表
        """
        try:
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
        except ValueError:
            return [DateRange(start, end)]

        ranges = []
        current_date = start_date

        while current_date <= end_date:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            range_end = min(chunk_end, end_date)

            ranges.append(DateRange(
                current_date.strftime('%Y-%m-%d'),
                range_end.strftime('%Y-%m-%d')
            ))

            current_date = range_end + timedelta(days=1)

        return ranges

    @staticmethod
    def get_date_boundaries(ranges: List[DateRange]) -> Tuple[Optional[str], Optional[str]]:
        """
        获取日期范围列表的边界

        Args:
            ranges: 日期范围列表

        Returns:
            Tuple[Optional[str], Optional[str]]: (最早日期, 最晚日期)
        """
        if not ranges:
            return None, None

        start_dates = [r.start for r in ranges]
        end_dates = [r.end for r in ranges]

        return min(start_dates), max(end_dates)

    @staticmethod
    def filter_ranges_by_period(
        ranges: List[DateRange],
        start: str,
        end: str
    ) -> List[DateRange]:
        """
        过滤指定时间段内的日期范围

        Args:
            ranges: 日期范围列表
            start: 过滤开始日期
            end: 过滤结束日期

        Returns:
            List[DateRange]: 过滤后的日期范围列表
        """
        filter_range = DateRange(start, end)

        filtered_ranges = []
        for r in ranges:
            if r.overlaps(filter_range):
                # 计算交集
                intersection_start = max(r.start, filter_range.start)
                intersection_end = min(r.end, filter_range.end)
                filtered_ranges.append(DateRange(intersection_start, intersection_end))

        return filtered_ranges

    @staticmethod
    def calculate_coverage_ratio(
        target_range: DateRange,
        cached_ranges: List[DateRange]
    ) -> float:
        """
        计算缓存覆盖率

        Args:
            target_range: 目标范围
            cached_ranges: 已缓存范围列表

        Returns:
            float: 覆盖率 (0.0 - 1.0)
        """
        if not cached_ranges:
            return 0.0

        total_days = target_range.days_count()
        if total_days == 0:
            return 0.0

        # 计算覆盖的天数
        covered_ranges = RangeCalculator.filter_ranges_by_period(
            cached_ranges, target_range.start, target_range.end
        )
        merged_covered = RangeCalculator.merge_ranges(covered_ranges)

        covered_days = sum(r.days_count() for r in merged_covered)
        return min(covered_days / total_days, 1.0)

    @staticmethod
    def is_trading_date(date: str) -> bool:
        """
        简单的交易日判断（排除周末）
        实际使用中应该结合节假日数据

        Args:
            date: 日期字符串 (YYYY-MM-DD)

        Returns:
            bool: 是否为交易日
        """
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            # 周一到周五为交易日 (0-4)
            return date_obj.weekday() < 5
        except ValueError:
            return False

    @staticmethod
    def get_trading_dates_in_range(start: str, end: str) -> List[str]:
        """
        获取指定范围内的交易日列表

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            List[str]: 交易日列表
        """
        try:
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
        except ValueError:
            return []

        trading_dates = []
        current_date = start_date

        while current_date <= end_date:
            if RangeCalculator.is_trading_date(current_date.strftime('%Y-%m-%d')):
                trading_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)

        return trading_dates

    @staticmethod
    def _next_date(date: str) -> str:
        """获取下一天的日期"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            next_date = date_obj + timedelta(days=1)
            return next_date.strftime('%Y-%m-%d')
        except ValueError:
            return date

    @staticmethod
    def _previous_date(date: str) -> str:
        """获取前一天的日期"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            prev_date = date_obj - timedelta(days=1)
            return prev_date.strftime('%Y-%m-%d')
        except ValueError:
            return date