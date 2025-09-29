"""
主策略 - 最严格的选股条件
"""
import pandas as pd
from typing import Dict, Any
from .base_strategy import BaseStrategy


class MainStrategy(BaseStrategy):
    """主选股策略

    应用最严格的筛选条件：
    - 基本过滤：价格<100元，市值50-500亿
    - 趋势条件：连续上涨3天，3日累计涨幅控制
    - 均线条件：收盘价>MA5，MA5>MA10>MA20（多头排列）
    - 换手率：分层区间法或相对换手率法
    - 板块/大盘：辅助过滤条件
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.consecutive_days = config.get('consecutive_days', 3)
        self.turnover_strategy = config.get('turnover_strategy', 'tiered')

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用主策略筛选

        Args:
            data: 输入的股票数据

        Returns:
            符合主策略的股票数据
        """
        if data.empty:
            self.logger.warning("输入数据为空")
            return data

        self.logger.info(f"开始应用主策略，输入数据: {len(data)} 只股票")

        # 1. 基础条件过滤
        filtered_data = self._filter_basic_conditions(data)
        if filtered_data.empty:
            self.logger.info("基础条件过滤后无候选股票")
            return self._add_strategy_info(filtered_data, 0)

        # 2. 趋势条件过滤
        filtered_data = self._filter_trend_conditions(filtered_data, self.consecutive_days)
        if filtered_data.empty:
            self.logger.info("趋势条件过滤后无候选股票")
            return self._add_strategy_info(filtered_data, 0)

        # 3. 均线条件过滤（严格模式）
        filtered_data = self._filter_ma_conditions(filtered_data, strict=True)
        if filtered_data.empty:
            self.logger.info("均线条件过滤后无候选股票")
            return self._add_strategy_info(filtered_data, 0)

        # 4. 换手率条件过滤
        filtered_data = self._filter_turnover_conditions(filtered_data, self.turnover_strategy)
        if filtered_data.empty:
            self.logger.info("换手率条件过滤后无候选股票")
            return self._add_strategy_info(filtered_data, 0)

        # 5. 板块/大盘条件过滤（可选）
        if self.config.get('enable_market_filter', False):
            filtered_data = self._filter_market_conditions(filtered_data)

        # 6. 应用单日涨幅阈值过滤
        filtered_data = self._filter_daily_return_threshold(filtered_data)

        self.logger.info(f"主策略筛选完成，候选股票: {len(filtered_data)} 只")

        return self._add_strategy_info(filtered_data, 0)

    def _filter_daily_return_threshold(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        根据市值分层应用单日涨幅阈值过滤

        Args:
            data: 输入数据

        Returns:
            过滤后的数据
        """
        if data.empty or 'daily_return' not in data.columns:
            return data

        filtered_rows = []

        for _, row in data.iterrows():
            market_value = row.get('market_value_billion', 100)  # 默认中盘
            daily_return = row['daily_return']

            # 根据市值确定涨幅阈值
            if market_value < 100:  # 小盘股
                min_threshold = self.config.get('small_cap_daily_return_threshold', 0.015)  # 1.5%
            elif market_value <= 500:  # 中盘股
                min_threshold = self.config.get('mid_cap_daily_return_threshold', 0.01)  # 1.0%
            else:  # 大盘股
                min_threshold = self.config.get('large_cap_daily_return_threshold', 0.005)  # 0.5%

            if daily_return >= min_threshold:
                filtered_rows.append(row)

        if filtered_rows:
            result = pd.DataFrame(filtered_rows)
            self.logger.debug(f"单日涨幅阈值过滤: {len(data)} -> {len(result)}")
            return result
        else:
            return pd.DataFrame(columns=data.columns)

    def _get_max_cumulative_return(self, data: pd.DataFrame) -> float:
        """
        根据市值分层获取最大累计涨幅

        Args:
            data: 股票数据

        Returns:
            最大累计涨幅
        """
        # 对于主策略，使用相对严格的累计涨幅限制
        cumulative_return_limits = self.config.get('cumulative_return_limits', {
            'small_cap': 0.08,   # 小盘股8%
            'mid_cap': 0.05,     # 中盘股5%
            'large_cap': 0.03    # 大盘股3%
        })

        # 返回中盘股标准作为默认值
        return cumulative_return_limits.get('mid_cap', 0.05)

    def _filter_market_conditions(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        应用板块/大盘条件过滤

        Args:
            data: 输入数据

        Returns:
            过滤后的数据
        """
        # 这里可以根据大盘和板块条件进行过滤
        # 由于需要额外的大盘和板块数据，暂时保持原数据不变
        self.logger.debug("板块/大盘条件过滤暂未实现")
        return data

    def get_name(self) -> str:
        """获取策略名称"""
        return "主策略"

    def get_description(self) -> str:
        """获取策略描述"""
        return (
            f"主选股策略 - 连续上涨{self.consecutive_days}天，"
            f"多头排列，{self.turnover_strategy}换手率策略"
        )

    def get_filter_summary(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        获取筛选结果摘要

        Args:
            data: 筛选结果数据

        Returns:
            筛选摘要信息
        """
        if data.empty:
            return {
                'strategy_name': self.get_name(),
                'candidate_count': 0,
                'avg_consecutive_days': 0,
                'avg_3d_return': 0,
                'avg_turnover': 0
            }

        return {
            'strategy_name': self.get_name(),
            'candidate_count': len(data),
            'avg_consecutive_days': data['consecutive_up_days'].mean() if 'consecutive_up_days' in data.columns else 0,
            'avg_3d_return': data['return_3d'].mean() if 'return_3d' in data.columns else 0,
            'avg_turnover': data['turnover'].mean() if 'turnover' in data.columns else 0,
            'market_value_range': {
                'min': data['market_value_billion'].min() if 'market_value_billion' in data.columns else 0,
                'max': data['market_value_billion'].max() if 'market_value_billion' in data.columns else 0,
                'avg': data['market_value_billion'].mean() if 'market_value_billion' in data.columns else 0
            }
        }