"""
稳定性因子 - 基于价格波动稳定性的评分因子
"""
import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_factor import BaseFactor


class StabilityFactor(BaseFactor):
    """稳定性因子

    基于价格波动稳定性指标计算得分：
    - 最近3日日振幅 (越小越好)
    - 收盘价标准差 (越小越好)
    - 极端单日涨跌幅检测 (>5%扣分)
    - 价格连续性 (避免跳空缺口)
    """

    def __init__(self, weight: float = 0.25, config: Dict[str, Any] = None):
        super().__init__(weight, config)
        self.extreme_threshold = config.get('extreme_threshold', 0.05) if config else 0.05  # 5%

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算稳定性因子得分

        Args:
            data: 股票数据

        Returns:
            稳定性因子得分Series
        """
        if data.empty:
            return pd.Series(dtype=float)

        scores = pd.Series(0.0, index=data.index)
        total_weight = 0.0

        try:
            # 日振幅得分 (权重: 35%)
            if self._has_amplitude_data(data):
                amplitude_scores = self._calculate_amplitude_score(data)
                scores += amplitude_scores * 0.35
                total_weight += 0.35

            # 价格波动稳定性得分 (权重: 30%)
            if self._has_volatility_data(data):
                volatility_scores = self._calculate_volatility_score(data)
                scores += volatility_scores * 0.30
                total_weight += 0.30

            # 极端波动惩罚得分 (权重: 25%)
            if 'daily_return' in data.columns:
                extreme_scores = self._calculate_extreme_volatility_score(data)
                scores += extreme_scores * 0.25
                total_weight += 0.25

            # 价格连续性得分 (权重: 10%)
            if self._has_gap_data(data):
                continuity_scores = self._calculate_price_continuity_score(data)
                scores += continuity_scores * 0.10
                total_weight += 0.10

            # 标准化得分
            if total_weight > 0:
                scores = scores / total_weight
            else:
                scores = pd.Series(0.5, index=data.index)

            # 处理缺失值
            scores = self.handle_missing_values(scores, method='median')

            self.logger.info(f"稳定性因子计算完成，有效样本: {scores.count()}")

        except Exception as e:
            self.logger.error(f"稳定性因子计算失败: {str(e)}")
            scores = pd.Series(0.5, index=data.index)

        return scores

    def _has_amplitude_data(self, data: pd.DataFrame) -> bool:
        """检查是否有振幅数据"""
        return any(col in data.columns for col in ['daily_amplitude', 'high', 'low', 'close'])

    def _has_volatility_data(self, data: pd.DataFrame) -> bool:
        """检查是否有波动率数据"""
        return any(col in data.columns for col in ['close_std_3d', 'close'])

    def _has_gap_data(self, data: pd.DataFrame) -> bool:
        """检查是否有跳空数据"""
        return all(col in data.columns for col in ['open', 'close', 'high', 'low'])

    def _calculate_amplitude_score(self, data: pd.DataFrame) -> pd.Series:
        """计算振幅得分 (振幅越小得分越高)"""
        if 'daily_amplitude' in data.columns:
            amplitude = data['daily_amplitude']
        elif all(col in data.columns for col in ['high', 'low', 'close']):
            # 计算日振幅：(最高价-最低价)/前收盘价
            if 'preclose' in data.columns:
                amplitude = (data['high'] - data['low']) / data['preclose']
            else:
                amplitude = (data['high'] - data['low']) / data['close']
        else:
            return pd.Series(0.5, index=data.index)

        # 振幅评分：振幅越小得分越高
        amplitude = amplitude.fillna(0.05)  # 默认5%振幅

        scores = pd.Series(index=data.index, dtype=float)

        # 振幅很小 (< 2%)
        scores[amplitude < 0.02] = 1.0

        # 振幅较小 (2%-4%)
        mask = (amplitude >= 0.02) & (amplitude < 0.04)
        scores[mask] = 1.0 - (amplitude[mask] - 0.02) * 10  # 1.0-0.8

        # 振幅适中 (4%-6%)
        mask = (amplitude >= 0.04) & (amplitude < 0.06)
        scores[mask] = 0.8 - (amplitude[mask] - 0.04) * 15  # 0.8-0.5

        # 振幅较大 (6%-10%)
        mask = (amplitude >= 0.06) & (amplitude < 0.10)
        scores[mask] = 0.5 - (amplitude[mask] - 0.06) * 7.5  # 0.5-0.2

        # 振幅很大 (>= 10%)
        scores[amplitude >= 0.10] = 0.1

        return scores.fillna(0.5)

    def _calculate_volatility_score(self, data: pd.DataFrame) -> pd.Series:
        """计算价格波动稳定性得分"""
        if 'close_std_3d' in data.columns:
            volatility = data['close_std_3d'] / data['close']  # 相对标准差
        elif 'close' in data.columns:
            # 如果没有预计算的标准差，计算相对变化
            returns = data['daily_return'] if 'daily_return' in data.columns else pd.Series(0.02, index=data.index)
            volatility = returns.abs()
        else:
            return pd.Series(0.5, index=data.index)

        volatility = volatility.fillna(0.02)

        # 波动率评分：波动越小得分越高
        scores = pd.Series(index=data.index, dtype=float)

        # 波动很小 (< 1%)
        scores[volatility < 0.01] = 1.0

        # 波动较小 (1%-2%)
        mask = (volatility >= 0.01) & (volatility < 0.02)
        scores[mask] = 1.0 - (volatility[mask] - 0.01) * 20  # 1.0-0.8

        # 波动适中 (2%-3%)
        mask = (volatility >= 0.02) & (volatility < 0.03)
        scores[mask] = 0.8 - (volatility[mask] - 0.02) * 30  # 0.8-0.5

        # 波动较大 (3%-5%)
        mask = (volatility >= 0.03) & (volatility < 0.05)
        scores[mask] = 0.5 - (volatility[mask] - 0.03) * 15  # 0.5-0.2

        # 波动很大 (>= 5%)
        scores[volatility >= 0.05] = 0.1

        return scores.fillna(0.5)

    def _calculate_extreme_volatility_score(self, data: pd.DataFrame) -> pd.Series:
        """计算极端波动惩罚得分"""
        daily_return = data['daily_return'].fillna(0)

        # 检测极端波动
        extreme_volatility = np.abs(daily_return) > self.extreme_threshold

        # 基础得分
        scores = pd.Series(0.8, index=data.index)  # 基础得分0.8

        # 有极端波动的股票扣分
        scores[extreme_volatility] = 0.2

        # 对于涨跌幅适中的股票给予奖励
        moderate_mask = (np.abs(daily_return) >= 0.01) & (np.abs(daily_return) <= 0.03)
        scores[moderate_mask] = 1.0

        return scores

    def _calculate_price_continuity_score(self, data: pd.DataFrame) -> pd.Series:
        """计算价格连续性得分 (检测跳空缺口)"""
        scores = pd.Series(0.8, index=data.index)  # 基础得分

        if not all(col in data.columns for col in ['open', 'close', 'high', 'low']):
            return scores

        # 检测向上跳空
        if 'preclose' in data.columns:
            gap_up = data['low'] > data['preclose']
            gap_down = data['high'] < data['preclose']
        else:
            # 使用开盘价检测跳空
            prev_close = data['close'].shift(1)
            gap_up = data['low'] > prev_close
            gap_down = data['high'] < prev_close

        # 计算缺口大小
        if 'preclose' in data.columns:
            gap_size_up = (data['low'] - data['preclose']) / data['preclose']
            gap_size_down = (data['preclose'] - data['high']) / data['preclose']
        else:
            prev_close = data['close'].shift(1)
            gap_size_up = (data['low'] - prev_close) / prev_close
            gap_size_down = (prev_close - data['high']) / prev_close

        # 小缺口 (< 2%) 影响较小
        small_gap_up = gap_up & (gap_size_up < 0.02)
        small_gap_down = gap_down & (gap_size_down < 0.02)
        scores[small_gap_up | small_gap_down] = 0.7

        # 大缺口 (>= 2%) 影响较大
        big_gap_up = gap_up & (gap_size_up >= 0.02)
        big_gap_down = gap_down & (gap_size_down >= 0.02)
        scores[big_gap_up | big_gap_down] = 0.3

        # 无缺口的给予奖励
        no_gap = ~gap_up & ~gap_down
        scores[no_gap] = 1.0

        return scores.fillna(0.8)

    def get_name(self) -> str:
        return "稳定性因子"

    def get_required_columns(self) -> list:
        return ['high', 'low', 'close']

    def get_description(self) -> str:
        return (
            f"稳定性因子 - 权重: {self.weight:.2%}\n"
            f"基于日振幅、价格波动、极端波动惩罚等指标，偏好稳定上涨的股票\n"
            f"极端波动阈值: {self.extreme_threshold:.1%}"
        )

    def analyze_stability_characteristics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析稳定性特征"""
        if data.empty:
            return {}

        analysis = {}

        # 振幅分析
        if 'daily_amplitude' in data.columns or all(col in data.columns for col in ['high', 'low', 'close']):
            if 'daily_amplitude' in data.columns:
                amplitude = data['daily_amplitude']
            else:
                amplitude = (data['high'] - data['low']) / data['close']

            analysis['amplitude'] = {
                'mean': amplitude.mean(),
                'std': amplitude.std(),
                'median': amplitude.median(),
                'low_volatility_ratio': (amplitude < 0.03).mean()  # 振幅小于3%的比例
            }

        # 极端波动分析
        if 'daily_return' in data.columns:
            daily_return = data['daily_return']
            extreme_count = (np.abs(daily_return) > self.extreme_threshold).sum()

            analysis['extreme_volatility'] = {
                'extreme_count': extreme_count,
                'extreme_ratio': extreme_count / len(data),
                'max_positive_return': daily_return.max(),
                'max_negative_return': daily_return.min()
            }

        # 价格连续性分析
        if all(col in data.columns for col in ['open', 'close', 'high', 'low']):
            if 'preclose' in data.columns:
                gap_up = (data['low'] > data['preclose']).sum()
                gap_down = (data['high'] < data['preclose']).sum()
            else:
                prev_close = data['close'].shift(1)
                gap_up = (data['low'] > prev_close).sum()
                gap_down = (data['high'] < prev_close).sum()

            analysis['price_continuity'] = {
                'gap_up_count': gap_up,
                'gap_down_count': gap_down,
                'total_gaps': gap_up + gap_down,
                'continuity_ratio': 1 - (gap_up + gap_down) / len(data)
            }

        return analysis

    def get_factor_details(self) -> Dict[str, Any]:
        """获取因子详细信息"""
        return {
            'name': self.get_name(),
            'weight': self.weight,
            'components': {
                'amplitude': {'weight': 0.35, 'description': '日振幅，越小越好'},
                'volatility': {'weight': 0.30, 'description': '价格波动稳定性'},
                'extreme_penalty': {'weight': 0.25, 'description': '极端波动惩罚'},
                'continuity': {'weight': 0.10, 'description': '价格连续性'}
            },
            'score_range': [0.0, 1.0],
            'preferences': {
                'daily_amplitude': '< 3%',
                'daily_volatility': '< 2%',
                'extreme_moves': f'< {self.extreme_threshold:.1%}',
                'price_gaps': 'minimal'
            },
            'extreme_threshold': self.extreme_threshold
        }