"""
回退策略 - 在主策略无候选时启用的放宽策略
"""
import pandas as pd
from typing import Dict, Any
from .base_strategy import BaseStrategy


class FallbackStrategy1(BaseStrategy):
    """回退策略1 - 轻微放宽条件

    相比主策略的放宽：
    - 连续上涨天数降为2天
    - 均线条件放宽为MA5>MA10（不强制MA20）
    - 换手率条件适当放宽
    - 累计涨幅上限适当放宽（+20%）
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.consecutive_days = config.get('fallback1_consecutive_days', 2)
        self.return_limit_boost = config.get('fallback1_return_limit_boost', 0.2)  # 放宽20%

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用回退策略1"""
        if data.empty:
            return data

        self.logger.info(f"开始应用回退策略1，输入数据: {len(data)} 只股票")

        # 1. 基础条件过滤
        filtered_data = self._filter_basic_conditions(data)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 1)

        # 2. 趋势条件过滤（放宽连续上涨天数）
        filtered_data = self._filter_trend_conditions(filtered_data, self.consecutive_days)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 1)

        # 3. 均线条件过滤（宽松模式）
        filtered_data = self._filter_ma_conditions(filtered_data, strict=False)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 1)

        # 4. 换手率条件过滤（允许更多策略）
        filtered_data = self._filter_turnover_conditions_relaxed(filtered_data)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 1)

        self.logger.info(f"回退策略1筛选完成，候选股票: {len(filtered_data)} 只")

        return self._add_strategy_info(filtered_data, 1)

    def _get_max_cumulative_return(self, data: pd.DataFrame) -> float:
        """获取放宽后的最大累计涨幅"""
        base_limit = super()._get_max_cumulative_return(data)
        return base_limit * (1 + self.return_limit_boost)

    def _filter_turnover_conditions_relaxed(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用放宽的换手率条件"""
        # 允许满足分层区间法或相对换手率法中的任意一种
        tiered_result = self._apply_tiered_turnover_filter(data)
        relative_result = self._apply_relative_turnover_filter(data)

        # 合并两种结果
        if not tiered_result.empty and not relative_result.empty:
            combined = pd.concat([tiered_result, relative_result]).drop_duplicates(subset=['code'])
        elif not tiered_result.empty:
            combined = tiered_result
        elif not relative_result.empty:
            combined = relative_result
        else:
            combined = pd.DataFrame(columns=data.columns)

        self.logger.debug(f"放宽换手率过滤: {len(data)} -> {len(combined)}")
        return combined

    def get_name(self) -> str:
        return "回退策略1"


class FallbackStrategy2(BaseStrategy):
    """回退策略2 - 进一步放宽条件

    相比回退策略1的进一步放宽：
    - 不强制连续上涨，改为当日收盘>MA10且当日上涨
    - 换手率只需高于行业/板块中位数或满足相对换手率的更宽松版本
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用回退策略2"""
        if data.empty:
            return data

        self.logger.info(f"开始应用回退策略2，输入数据: {len(data)} 只股票")

        # 1. 基础条件过滤
        filtered_data = self._filter_basic_conditions(data)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 2)

        # 2. 简化趋势条件：当日收盘>MA10且当日上涨
        filtered_data = self._filter_simple_trend_conditions(filtered_data)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 2)

        # 3. 宽松换手率条件
        filtered_data = self._filter_loose_turnover_conditions(filtered_data)
        if filtered_data.empty:
            return self._add_strategy_info(filtered_data, 2)

        self.logger.info(f"回退策略2筛选完成，候选股票: {len(filtered_data)} 只")

        return self._add_strategy_info(filtered_data, 2)

    def _filter_simple_trend_conditions(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用简化的趋势条件"""
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        # 当日收盘价 > MA10
        if 'close' in filtered_data.columns and 'ma10' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['close'] > filtered_data['ma10']]
            self.logger.debug(f"收盘价>MA10过滤: {len(filtered_data)}")

        # 当日上涨
        if 'daily_return' in filtered_data.columns:
            filtered_data = filtered_data[filtered_data['daily_return'] > 0]
            self.logger.debug(f"当日上涨过滤: {len(filtered_data)}")

        final_count = len(filtered_data)
        self.logger.info(f"简化趋势条件过滤: {initial_count} -> {final_count}")

        return filtered_data

    def _filter_loose_turnover_conditions(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用宽松的换手率条件"""
        if data.empty:
            return data

        # 使用更宽松的相对换手率标准
        if 'relative_turnover' in data.columns:
            # 相对换手率 > 1.0（高于历史平均）
            return data[data['relative_turnover'] > 1.0]
        else:
            # 如果没有相对换手率数据，直接返回原数据
            return data

    def get_name(self) -> str:
        return "回退策略2"


class EmergencyStrategy(BaseStrategy):
    """兜底策略 - 保证非空的最宽松策略

    仅要求：
    - 市值范围在配置范围内
    - 股价 < 100元
    - 有基本的成交量
    - 依赖评分引擎排序选出候选股票
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.max_candidates = config.get('emergency_max_candidates', 50)

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用兜底策略"""
        if data.empty:
            return data

        self.logger.info(f"开始应用兜底策略，输入数据: {len(data)} 只股票")

        # 只应用最基础的过滤条件
        filtered_data = self._filter_basic_conditions(data)

        # 如果还是没有候选股票，进一步放宽条件
        if filtered_data.empty:
            filtered_data = self._filter_emergency_conditions(data)

        # 限制候选股票数量，避免返回过多股票
        if len(filtered_data) > self.max_candidates:
            # 如果有评分，按评分排序；否则按成交额排序
            if 'total_score' in filtered_data.columns:
                filtered_data = filtered_data.nlargest(self.max_candidates, 'total_score')
            elif 'amount' in filtered_data.columns:
                filtered_data = filtered_data.nlargest(self.max_candidates, 'amount')
            else:
                filtered_data = filtered_data.head(self.max_candidates)

        self.logger.info(f"兜底策略筛选完成，候选股票: {len(filtered_data)} 只")

        return self._add_strategy_info(filtered_data, 3)

    def _filter_emergency_conditions(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用紧急兜底条件"""
        if data.empty:
            return data

        filtered_data = data.copy()
        initial_count = len(filtered_data)

        # 仅保留最基本的条件
        # 价格过滤
        if 'close' in filtered_data.columns:
            max_price = self.config.get('emergency_max_price', 200.0)  # 放宽到200元
            filtered_data = filtered_data[filtered_data['close'] <= max_price]

        # 成交量过滤（确保有基本流动性）
        if 'volume' in filtered_data.columns:
            min_volume = self.config.get('emergency_min_volume', 100)  # 极低的成交量要求
            filtered_data = filtered_data[filtered_data['volume'] >= min_volume]

        # 市值过滤（放宽范围）
        if 'market_value_billion' in filtered_data.columns:
            min_mv = self.config.get('emergency_min_market_value', 10)  # 10亿
            max_mv = self.config.get('emergency_max_market_value', 1000)  # 1000亿
            filtered_data = filtered_data[
                (filtered_data['market_value_billion'] >= min_mv) &
                (filtered_data['market_value_billion'] <= max_mv)
            ]

        final_count = len(filtered_data)
        self.logger.info(f"紧急兜底条件过滤: {initial_count} -> {final_count}")

        return filtered_data

    def get_name(self) -> str:
        return "兜底策略"

    def get_description(self) -> str:
        return f"兜底策略 - 保证非空，最多返回{self.max_candidates}只候选股票"