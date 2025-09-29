"""
价格位置因子 - 基于价格在历史区间位置的评分因子
"""
import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_factor import BaseFactor


class PricePositionFactor(BaseFactor):
    """价格位置因子

    基于价格在历史区间位置计算得分：
    - 当前价格在过去N日区间的位置 (低位得高分)
    - 相对于历史均价的位置
    - 支撑阻力位分析
    - 回调幅度分析
    """

    def __init__(self, weight: float = 0.2, config: Dict[str, Any] = None):
        super().__init__(weight, config)
        self.lookback_period = config.get('lookback_period', 252) if config else 252  # 252个交易日约1年
        self.position_preference = config.get('position_preference', 'low') if config else 'low'  # 'low', 'mid', 'high'

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算价格位置因子得分

        Args:
            data: 股票数据

        Returns:
            价格位置因子得分Series
        """
        if data.empty:
            return pd.Series(dtype=float)

        scores = pd.Series(0.0, index=data.index)
        total_weight = 0.0

        try:
            # 历史区间位置得分 (权重: 50%)
            if self._has_price_position_data(data):
                position_scores = self._calculate_position_score(data)
                scores += position_scores * 0.50
                total_weight += 0.50

            # 相对均价位置得分 (权重: 30%)
            if self._has_average_price_data(data):
                avg_price_scores = self._calculate_average_price_score(data)
                scores += avg_price_scores * 0.30
                total_weight += 0.30

            # 回调幅度得分 (权重: 20%)
            if self._has_pullback_data(data):
                pullback_scores = self._calculate_pullback_score(data)
                scores += pullback_scores * 0.20
                total_weight += 0.20

            # 标准化得分
            if total_weight > 0:
                scores = scores / total_weight
            else:
                scores = pd.Series(0.5, index=data.index)

            # 处理缺失值
            scores = self.handle_missing_values(scores, method='median')

            self.logger.info(f"价格位置因子计算完成，有效样本: {scores.count()}")

        except Exception as e:
            self.logger.error(f"价格位置因子计算失败: {str(e)}")
            scores = pd.Series(0.5, index=data.index)

        return scores

    def _has_price_position_data(self, data: pd.DataFrame) -> bool:
        """检查是否有价格位置数据"""
        return any(col in data.columns for col in ['price_position', 'highest_price', 'lowest_price']) or 'close' in data.columns

    def _has_average_price_data(self, data: pd.DataFrame) -> bool:
        """检查是否有均价数据"""
        return any(col in data.columns for col in ['ma20', 'ma60', 'close'])

    def _has_pullback_data(self, data: pd.DataFrame) -> bool:
        """检查是否有回调数据"""
        return any(col in data.columns for col in ['highest_price', 'close', 'return_5d'])

    def _calculate_position_score(self, data: pd.DataFrame) -> pd.Series:
        """计算历史区间位置得分"""
        if 'price_position' in data.columns:
            # 使用预计算的价格位置
            position = data['price_position']
        elif all(col in data.columns for col in ['close', 'highest_price', 'lowest_price']):
            # 计算价格位置
            price_range = data['highest_price'] - data['lowest_price']
            position = np.where(
                price_range > 0,
                (data['close'] - data['lowest_price']) / price_range,
                0.5
            )
            position = pd.Series(position, index=data.index)
        else:
            return pd.Series(0.5, index=data.index)

        # 根据位置偏好计算得分
        if self.position_preference == 'low':
            # 偏好低位 (价值投资风格)
            scores = self._calculate_low_position_score(position)
        elif self.position_preference == 'mid':
            # 偏好中位 (平衡风格)
            scores = self._calculate_mid_position_score(position)
        elif self.position_preference == 'high':
            # 偏好高位 (动量风格)
            scores = self._calculate_high_position_score(position)
        else:
            # 默认偏好低位
            scores = self._calculate_low_position_score(position)

        return scores.fillna(0.5)

    def _calculate_low_position_score(self, position: pd.Series) -> pd.Series:
        """计算低位偏好得分 (位置越低得分越高)"""
        scores = pd.Series(index=position.index, dtype=float)

        # 极低位 (0-20%)
        mask = position <= 0.2
        scores[mask] = 1.0

        # 低位 (20%-40%)
        mask = (position > 0.2) & (position <= 0.4)
        scores[mask] = 1.0 - (position[mask] - 0.2) * 2.5  # 1.0-0.5

        # 中位 (40%-60%)
        mask = (position > 0.4) & (position <= 0.6)
        scores[mask] = 0.5 - (position[mask] - 0.4) * 1.5  # 0.5-0.2

        # 高位 (60%-80%)
        mask = (position > 0.6) & (position <= 0.8)
        scores[mask] = 0.2 - (position[mask] - 0.6) * 0.5  # 0.2-0.1

        # 极高位 (80%-100%)
        scores[position > 0.8] = 0.05

        return scores

    def _calculate_mid_position_score(self, position: pd.Series) -> pd.Series:
        """计算中位偏好得分 (中间位置得分最高)"""
        scores = pd.Series(index=position.index, dtype=float)

        # 使用倒U型曲线
        center = 0.5
        scores = 1.0 - 2 * np.abs(position - center)
        scores = scores.clip(0.1, 1.0)

        return pd.Series(scores, index=position.index)

    def _calculate_high_position_score(self, position: pd.Series) -> pd.Series:
        """计算高位偏好得分 (位置越高得分越高)"""
        scores = pd.Series(index=position.index, dtype=float)

        # 极低位 (0-20%)
        scores[position <= 0.2] = 0.05

        # 低位 (20%-40%)
        mask = (position > 0.2) & (position <= 0.4)
        scores[mask] = 0.1 + (position[mask] - 0.2) * 0.5  # 0.1-0.2

        # 中位 (40%-60%)
        mask = (position > 0.4) & (position <= 0.6)
        scores[mask] = 0.2 + (position[mask] - 0.4) * 1.5  # 0.2-0.5

        # 高位 (60%-80%)
        mask = (position > 0.6) & (position <= 0.8)
        scores[mask] = 0.5 + (position[mask] - 0.6) * 2.5  # 0.5-1.0

        # 极高位 (80%-100%)
        scores[position > 0.8] = 1.0

        return scores

    def _calculate_average_price_score(self, data: pd.DataFrame) -> pd.Series:
        """计算相对均价位置得分"""
        scores = pd.Series(0.5, index=data.index)

        if 'close' not in data.columns:
            return scores

        close_price = data['close']

        # 优先使用长期均线
        if 'ma60' in data.columns:
            avg_price = data['ma60']
        elif 'ma20' in data.columns:
            avg_price = data['ma20']
        else:
            # 使用整体均价
            avg_price = pd.Series(close_price.mean(), index=data.index)

        # 计算相对于均价的位置
        price_ratio = close_price / avg_price

        # 根据偏好计算得分
        if self.position_preference == 'low':
            # 偏好低于均价的股票
            scores = pd.Series(index=data.index, dtype=float)

            # 远低于均价 (< 0.85)
            scores[price_ratio < 0.85] = 1.0

            # 略低于均价 (0.85-0.95)
            mask = (price_ratio >= 0.85) & (price_ratio < 0.95)
            scores[mask] = 1.0 - (price_ratio[mask] - 0.85) * 5  # 1.0-0.5

            # 接近均价 (0.95-1.05)
            mask = (price_ratio >= 0.95) & (price_ratio <= 1.05)
            scores[mask] = 0.5

            # 高于均价 (> 1.05)
            scores[price_ratio > 1.05] = 0.3

        else:
            # 其他偏好使用均衡评分
            scores = pd.Series(index=data.index, dtype=float)

            # 远离均价的扣分
            deviation = np.abs(price_ratio - 1.0)
            scores = 1.0 - deviation * 2
            scores = scores.clip(0.1, 1.0)

        return scores.fillna(0.5)

    def _calculate_pullback_score(self, data: pd.DataFrame) -> pd.Series:
        """计算回调幅度得分"""
        scores = pd.Series(0.5, index=data.index)

        if 'close' not in data.columns:
            return scores

        # 计算回调幅度
        if 'highest_price' in data.columns:
            # 从最高价的回调
            pullback = (data['highest_price'] - data['close']) / data['highest_price']
        elif 'return_5d' in data.columns:
            # 使用5日收益率的相反数作为回调指标
            pullback = -data['return_5d']
        else:
            return scores

        pullback = pullback.fillna(0)

        # 回调评分 (适度回调有利于后续上涨)
        scores = pd.Series(index=data.index, dtype=float)

        # 无回调或小幅上涨 (回调 < 0)
        scores[pullback < 0] = 0.6

        # 小幅回调 (0-5%)
        mask = (pullback >= 0) & (pullback < 0.05)
        scores[mask] = 0.6 + pullback[mask] * 8  # 0.6-1.0

        # 适度回调 (5%-15%)
        mask = (pullback >= 0.05) & (pullback < 0.15)
        scores[mask] = 1.0 - (pullback[mask] - 0.05) * 3  # 1.0-0.7

        # 大幅回调 (15%-30%)
        mask = (pullback >= 0.15) & (pullback < 0.30)
        scores[mask] = 0.7 - (pullback[mask] - 0.15) * 2  # 0.7-0.4

        # 巨幅回调 (>= 30%)
        scores[pullback >= 0.30] = 0.2

        return scores.fillna(0.5)

    def get_name(self) -> str:
        return "价格位置因子"

    def get_required_columns(self) -> list:
        return ['close']

    def get_description(self) -> str:
        return (
            f"价格位置因子 - 权重: {self.weight:.2%}\n"
            f"基于价格在{self.lookback_period}日历史区间的位置，偏好{self.position_preference}位股票\n"
            "结合均价位置和回调幅度综合评分"
        )

    def analyze_price_position_distribution(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析价格位置分布"""
        if data.empty or 'close' not in data.columns:
            return {}

        analysis = {}

        # 价格位置分布
        if 'price_position' in data.columns:
            position = data['price_position']
        elif all(col in data.columns for col in ['highest_price', 'lowest_price']):
            price_range = data['highest_price'] - data['lowest_price']
            position = np.where(
                price_range > 0,
                (data['close'] - data['lowest_price']) / price_range,
                0.5
            )
            position = pd.Series(position, index=data.index)
        else:
            position = pd.Series(0.5, index=data.index)

        analysis['position_distribution'] = {
            'mean': position.mean(),
            'median': position.median(),
            'std': position.std(),
            'low_position_ratio': (position <= 0.3).mean(),  # 低位股票比例
            'high_position_ratio': (position >= 0.7).mean()  # 高位股票比例
        }

        # 相对均价分析
        if 'ma20' in data.columns:
            price_to_ma = data['close'] / data['ma20']
            analysis['relative_to_ma20'] = {
                'mean': price_to_ma.mean(),
                'below_ma_ratio': (price_to_ma < 1.0).mean(),
                'significantly_below_ratio': (price_to_ma < 0.9).mean()
            }

        return analysis

    def update_position_preference(self, preference: str):
        """更新位置偏好"""
        if preference in ['low', 'mid', 'high']:
            old_preference = self.position_preference
            self.position_preference = preference
            self.logger.info(f"价格位置偏好更新: {old_preference} -> {preference}")
        else:
            self.logger.warning(f"无效的位置偏好: {preference}")

    def get_factor_details(self) -> Dict[str, Any]:
        """获取因子详细信息"""
        return {
            'name': self.get_name(),
            'weight': self.weight,
            'components': {
                'historical_position': {'weight': 0.50, 'description': f'{self.lookback_period}日历史区间位置'},
                'relative_to_average': {'weight': 0.30, 'description': '相对均价位置'},
                'pullback_analysis': {'weight': 0.20, 'description': '回调幅度分析'}
            },
            'score_range': [0.0, 1.0],
            'parameters': {
                'lookback_period': self.lookback_period,
                'position_preference': self.position_preference
            },
            'optimal_ranges': {
                'low_preference': {'position': [0.0, 0.3], 'pullback': [0.05, 0.15]},
                'mid_preference': {'position': [0.3, 0.7], 'pullback': [0.0, 0.10]},
                'high_preference': {'position': [0.7, 1.0], 'pullback': [0.0, 0.05]}
            }
        }