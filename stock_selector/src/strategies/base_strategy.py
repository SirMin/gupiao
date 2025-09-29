"""
策略基类 - 定义选股策略的统一接口
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any
import logging


class BaseStrategy(ABC):
    """选股策略基类

    定义所有选股策略的统一接口，包括策略应用和参数配置
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化策略

        Args:
            config: 策略配置参数
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用策略筛选

        Args:
            data: 输入的股票数据DataFrame

        Returns:
            符合策略条件的股票DataFrame
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        获取策略名称

        Returns:
            策略名称字符串
        """
        pass

    def get_description(self) -> str:
        """
        获取策略描述

        Returns:
            策略描述字符串
        """
        return f"{self.get_name()} - 基于配置参数的选股策略"

    def _filter_basic_conditions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用基础过滤条件

        Args:
            data: 输入数据

        Returns:
            过滤后的数据
        """
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        # 价格过滤
        max_price = self.config.get('max_price', 100.0)
        if 'close' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['close'] <= max_price]
            self.logger.debug(f"价格过滤 (<= {max_price}): {len(filtered_data)}")

        # 市值过滤
        min_market_value = self.config.get('min_market_value', 5e9)  # 50亿
        max_market_value = self.config.get('max_market_value', 5e10)  # 500亿

        if 'market_value_billion' in filtered_data.columns:
            # 市值以亿为单位
            min_mv_billion = min_market_value / 1e8
            max_mv_billion = max_market_value / 1e8
            filtered_data = filtered_data[
                (filtered_data['market_value_billion'] >= min_mv_billion) &
                (filtered_data['market_value_billion'] <= max_mv_billion)
            ]
            self.logger.debug(f"市值过滤 ({min_mv_billion:.0f}-{max_mv_billion:.0f}亿): {len(filtered_data)}")

        # 成交量过滤（避免流动性过低的股票）
        if 'volume' in filtered_data.columns:
            min_volume = self.config.get('min_volume', 1000)  # 最小成交量
            filtered_data = filtered_data[filtered_data['volume'] >= min_volume]
            self.logger.debug(f"成交量过滤 (>= {min_volume}): {len(filtered_data)}")

        final_count = len(filtered_data)
        self.logger.info(f"基础条件过滤: {initial_count} -> {final_count}")

        return filtered_data

    def _filter_trend_conditions(self, data: pd.DataFrame, consecutive_days: int = 3) -> pd.DataFrame:
        """
        应用趋势条件过滤

        Args:
            data: 输入数据
            consecutive_days: 连续上涨天数要求

        Returns:
            过滤后的数据
        """
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        # 连续上涨天数过滤
        if 'consecutive_up_days' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['consecutive_up_days'] >= consecutive_days]
            self.logger.debug(f"连续上涨天数过滤 (>= {consecutive_days}): {len(filtered_data)}")

        # 累计涨幅控制
        max_cumulative_return = self._get_max_cumulative_return(filtered_data)
        if 'return_3d' in filtered_data.columns and max_cumulative_return:
            filtered_data = filtered_data[filtered_data['return_3d'] <= max_cumulative_return]
            self.logger.debug(f"3日累计涨幅过滤 (<= {max_cumulative_return:.2%}): {len(filtered_data)}")

        final_count = len(filtered_data)
        self.logger.info(f"趋势条件过滤: {initial_count} -> {final_count}")

        return filtered_data

    def _get_max_cumulative_return(self, data: pd.DataFrame) -> float:
        """
        根据市值分层获取最大累计涨幅

        Args:
            data: 股票数据

        Returns:
            最大累计涨幅
        """
        # 默认中盘股标准
        return self.config.get('max_cumulative_return', 0.05)  # 5%

    def _filter_ma_conditions(self, data: pd.DataFrame, strict: bool = True) -> pd.DataFrame:
        """
        应用均线条件过滤

        Args:
            data: 输入数据
            strict: 是否应用严格的均线条件

        Returns:
            过滤后的数据
        """
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        # 收盘价高于5日均线
        if 'close_gt_ma5' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['close_gt_ma5']]
            self.logger.debug(f"收盘价>MA5过滤: {len(filtered_data)}")

        if strict:
            # 严格模式：MA5 > MA10 > MA20 (多头排列)
            if 'ma_bullish' in filtered_data.columns:
                filtered_data = filtered_data[filtered_data['ma_bullish']]
                self.logger.debug(f"多头排列过滤: {len(filtered_data)}")
        else:
            # 宽松模式：仅要求MA5 > MA10
            if 'ma5_gt_ma10' in filtered_data.columns:
                filtered_data = filtered_data[filtered_data['ma5_gt_ma10']]
                self.logger.debug(f"MA5>MA10过滤: {len(filtered_data)}")

        final_count = len(filtered_data)
        self.logger.info(f"均线条件过滤: {initial_count} -> {final_count}")

        return filtered_data

    def _filter_turnover_conditions(self, data: pd.DataFrame, strategy: str = "tiered") -> pd.DataFrame:
        """
        应用换手率条件过滤

        Args:
            data: 输入数据
            strategy: 换手率策略 ("tiered", "relative", "activity")

        Returns:
            过滤后的数据
        """
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        if strategy == "tiered":
            filtered_data = self._apply_tiered_turnover_filter(filtered_data)
        elif strategy == "relative":
            filtered_data = self._apply_relative_turnover_filter(filtered_data)
        elif strategy == "activity":
            filtered_data = self._apply_activity_turnover_filter(filtered_data)

        final_count = len(filtered_data)
        self.logger.info(f"换手率条件过滤 ({strategy}): {initial_count} -> {final_count}")

        return filtered_data

    def _apply_tiered_turnover_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用分层换手率过滤"""
        if 'turnover' not in data.columns or 'market_value_billion' not in data.columns:
            return data

        filtered_rows = []

        for _, row in data.iterrows():
            market_value = row['market_value_billion']
            turnover = row['turnover']
            cumulative_turnover = row.get('cumulative_turnover_3d', turnover * 3)

            # 市值分层
            if market_value < 100:  # 小盘股
                min_turn, max_turn, max_cum_turn = 0.05, 0.15, 0.30
            elif market_value <= 500:  # 中盘股
                min_turn, max_turn, max_cum_turn = 0.02, 0.08, 0.15
            else:  # 大盘股
                min_turn, max_turn, max_cum_turn = 0.01, 0.03, 0.08

            if min_turn <= turnover <= max_turn and cumulative_turnover <= max_cum_turn:
                filtered_rows.append(row)

        if filtered_rows:
            return pd.DataFrame(filtered_rows)
        else:
            return pd.DataFrame(columns=data.columns)

    def _apply_relative_turnover_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用相对换手率过滤"""
        if 'relative_turnover' not in data.columns:
            return data

        # 相对换手率在1.5-3倍之间
        return data[
            (data['relative_turnover'] >= 1.5) &
            (data['relative_turnover'] <= 3.0)
        ]

    def _apply_activity_turnover_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用活跃度过滤"""
        if 'activity_score' not in data.columns:
            return data

        # 设定活跃度区间（需要根据实际情况调整）
        min_activity = self.config.get('min_activity_score', 5)
        max_activity = self.config.get('max_activity_score', 50)

        return data[
            (data['activity_score'] >= min_activity) &
            (data['activity_score'] <= max_activity)
        ]

    def _add_strategy_info(self, data: pd.DataFrame, level: int) -> pd.DataFrame:
        """
        为结果数据添加策略信息

        Args:
            data: 筛选结果数据
            level: 策略级别

        Returns:
            添加了策略信息的数据
        """
        if not data.empty:
            data = data.copy()
            data['strategy_level'] = level
            data['strategy_name'] = self.get_name()

        return data

    def validate_config(self) -> bool:
        """
        验证策略配置

        Returns:
            配置是否有效
        """
        required_params = ['max_price', 'min_market_value', 'max_market_value']
        for param in required_params:
            if param not in self.config:
                self.logger.error(f"缺少必需的配置参数: {param}")
                return False

        return True