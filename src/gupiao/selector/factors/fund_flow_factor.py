"""
资金面因子 - 基于资金流动数据的评分因子
"""
import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_factor import BaseFactor


class FundFlowFactor(BaseFactor):
    """资金面因子

    基于资金流动指标计算得分：
    - 近3日主力净买入占流通市值比
    - 量比 (当日成交量/历史平均成交量)
    - 成交额比 (当日成交额/历史平均成交额)
    - 换手率活跃度
    """

    def __init__(self, weight: float = 0.25, config: Dict[str, Any] = None):
        super().__init__(weight, config)

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算资金面因子得分

        Args:
            data: 股票数据

        Returns:
            资金面因子得分Series
        """
        if data.empty:
            return pd.Series(dtype=float)

        scores = pd.Series(0.0, index=data.index)
        total_weight = 0.0

        try:
            # 量比得分 (权重: 30%)
            if self._has_volume_data(data):
                volume_scores = self._calculate_volume_ratio_score(data)
                scores += volume_scores * 0.30
                total_weight += 0.30

            # 成交额活跃度得分 (权重: 25%)
            if 'amount' in data.columns:
                amount_scores = self._calculate_amount_activity_score(data)
                scores += amount_scores * 0.25
                total_weight += 0.25

            # 换手率活跃度得分 (权重: 25%)
            if self._has_turnover_data(data):
                turnover_scores = self._calculate_turnover_activity_score(data)
                scores += turnover_scores * 0.25
                total_weight += 0.25

            # 主力资金净流入得分 (权重: 20%)
            if self._has_fund_flow_data(data):
                fund_flow_scores = self._calculate_fund_flow_score(data)
                scores += fund_flow_scores * 0.20
                total_weight += 0.20

            # 标准化得分
            if total_weight > 0:
                scores = scores / total_weight
            else:
                scores = pd.Series(0.5, index=data.index)

            # 处理缺失值
            scores = self.handle_missing_values(scores, method='median')

            self.logger.info(f"资金面因子计算完成，有效样本: {scores.count()}")

        except Exception as e:
            self.logger.error(f"资金面因子计算失败: {str(e)}")
            scores = pd.Series(0.5, index=data.index)

        return scores

    def _has_volume_data(self, data: pd.DataFrame) -> bool:
        """检查是否有成交量相关数据"""
        return 'volume' in data.columns

    def _has_turnover_data(self, data: pd.DataFrame) -> bool:
        """检查是否有换手率相关数据"""
        return any(col in data.columns for col in ['turnover', 'relative_turnover', 'avg_turnover_60d'])

    def _has_fund_flow_data(self, data: pd.DataFrame) -> bool:
        """检查是否有资金流数据"""
        return any(col in data.columns for col in ['net_inflow', 'main_net_inflow', 'fund_flow_ratio'])

    def _calculate_volume_ratio_score(self, data: pd.DataFrame) -> pd.Series:
        """计算量比得分"""
        scores = pd.Series(0.5, index=data.index)

        if 'volume' not in data.columns:
            return scores

        # 如果有历史平均成交量数据
        if 'avg_volume_60d' in data.columns:
            volume_ratio = data['volume'] / data['avg_volume_60d']
            volume_ratio = volume_ratio.fillna(1.0)
        else:
            # 使用相对成交量（当前成交量相对于整体平均的比例）
            avg_volume = data['volume'].mean()
            volume_ratio = data['volume'] / avg_volume if avg_volume > 0 else pd.Series(1.0, index=data.index)

        # 量比评分：1.5-3倍为最佳，过高过低都不好
        scores = pd.Series(index=data.index, dtype=float)

        # 量比太低 (< 0.5)
        scores[volume_ratio < 0.5] = 0.2

        # 量比偏低 (0.5-1.0)
        mask = (volume_ratio >= 0.5) & (volume_ratio < 1.0)
        scores[mask] = 0.3 + (volume_ratio[mask] - 0.5) * 0.4  # 0.3-0.5

        # 量比适中 (1.0-2.0)
        mask = (volume_ratio >= 1.0) & (volume_ratio <= 2.0)
        scores[mask] = 0.5 + (volume_ratio[mask] - 1.0) * 0.4  # 0.5-0.9

        # 量比较高 (2.0-4.0)
        mask = (volume_ratio > 2.0) & (volume_ratio <= 4.0)
        scores[mask] = 0.9 + (4.0 - volume_ratio[mask]) * 0.1 / 2.0  # 0.9-1.0

        # 量比过高 (> 4.0)
        scores[volume_ratio > 4.0] = 0.7  # 过度放量可能是出货

        return scores.fillna(0.5)

    def _calculate_amount_activity_score(self, data: pd.DataFrame) -> pd.Series:
        """计算成交额活跃度得分"""
        scores = pd.Series(0.5, index=data.index)

        if 'amount' not in data.columns:
            return scores

        # 使用成交额的相对活跃度
        if 'avg_amount_60d' in data.columns:
            amount_ratio = data['amount'] / data['avg_amount_60d']
        else:
            # 计算相对于平均成交额的比例
            median_amount = data['amount'].median()
            amount_ratio = data['amount'] / median_amount if median_amount > 0 else pd.Series(1.0, index=data.index)

        # 成交额活跃度评分
        scores = pd.Series(index=data.index, dtype=float)

        # 成交额太低
        scores[amount_ratio < 0.3] = 0.1

        # 成交额偏低
        mask = (amount_ratio >= 0.3) & (amount_ratio < 1.0)
        scores[mask] = 0.2 + (amount_ratio[mask] - 0.3) * 0.4  # 0.2-0.48

        # 成交额适中
        mask = (amount_ratio >= 1.0) & (amount_ratio <= 3.0)
        scores[mask] = 0.5 + (amount_ratio[mask] - 1.0) * 0.25  # 0.5-1.0

        # 成交额很高
        scores[amount_ratio > 3.0] = 0.9

        return scores.fillna(0.5)

    def _calculate_turnover_activity_score(self, data: pd.DataFrame) -> pd.Series:
        """计算换手率活跃度得分"""
        scores = pd.Series(0.5, index=data.index)

        # 优先使用相对换手率
        if 'relative_turnover' in data.columns:
            rel_turnover = data['relative_turnover']

            # 相对换手率评分：1.2-2.5倍为最佳
            scores = pd.Series(index=data.index, dtype=float)

            scores[rel_turnover < 0.8] = 0.2  # 太低

            mask = (rel_turnover >= 0.8) & (rel_turnover < 1.2)
            scores[mask] = 0.3 + (rel_turnover[mask] - 0.8) * 0.5  # 0.3-0.5

            mask = (rel_turnover >= 1.2) & (rel_turnover <= 2.5)
            scores[mask] = 0.6 + (rel_turnover[mask] - 1.2) * 0.3  # 0.6-1.0

            scores[rel_turnover > 2.5] = 0.8  # 过高可能有风险

        elif 'turnover' in data.columns:
            # 使用绝对换手率
            turnover = data['turnover']

            # 换手率评分（需要结合市值考虑）
            if 'market_value_billion' in data.columns:
                scores = self._calculate_tiered_turnover_score(data)
            else:
                # 使用统一标准
                scores = pd.Series(index=data.index, dtype=float)

                scores[turnover < 0.01] = 0.1  # < 1%

                mask = (turnover >= 0.01) & (turnover < 0.03)
                scores[mask] = 0.3 + (turnover[mask] - 0.01) * 10  # 0.3-0.5

                mask = (turnover >= 0.03) & (turnover <= 0.08)
                scores[mask] = 0.5 + (turnover[mask] - 0.03) * 6  # 0.5-0.8

                scores[turnover > 0.08] = 0.7  # > 8%

        return scores.fillna(0.5)

    def _calculate_tiered_turnover_score(self, data: pd.DataFrame) -> pd.Series:
        """基于市值分层的换手率评分"""
        scores = pd.Series(0.5, index=data.index)

        for idx, row in data.iterrows():
            market_value = row.get('market_value_billion', 100)
            turnover = row.get('turnover', 0.02)

            if market_value < 100:  # 小盘股
                if 0.03 <= turnover <= 0.12:
                    scores.loc[idx] = 0.6 + min((turnover - 0.03) / 0.09, 1.0) * 0.4
                elif turnover < 0.03:
                    scores.loc[idx] = 0.3
                else:
                    scores.loc[idx] = 0.5

            elif market_value <= 500:  # 中盘股
                if 0.015 <= turnover <= 0.06:
                    scores.loc[idx] = 0.6 + min((turnover - 0.015) / 0.045, 1.0) * 0.4
                elif turnover < 0.015:
                    scores.loc[idx] = 0.3
                else:
                    scores.loc[idx] = 0.5

            else:  # 大盘股
                if 0.008 <= turnover <= 0.025:
                    scores.loc[idx] = 0.6 + min((turnover - 0.008) / 0.017, 1.0) * 0.4
                elif turnover < 0.008:
                    scores.loc[idx] = 0.3
                else:
                    scores.loc[idx] = 0.5

        return scores

    def _calculate_fund_flow_score(self, data: pd.DataFrame) -> pd.Series:
        """计算资金流入得分"""
        scores = pd.Series(0.5, index=data.index)

        # 主力净流入比例
        if 'main_net_inflow' in data.columns and 'market_value_billion' in data.columns:
            # 主力净流入占流通市值比例
            net_inflow_ratio = data['main_net_inflow'] / (data['market_value_billion'] * 1e8)

            # 资金流入评分
            scores = pd.Series(index=data.index, dtype=float)

            # 大幅流出
            scores[net_inflow_ratio < -0.02] = 0.1

            # 小幅流出
            mask = (net_inflow_ratio >= -0.02) & (net_inflow_ratio < 0)
            scores[mask] = 0.3 + (net_inflow_ratio[mask] + 0.02) * 10  # 0.3-0.5

            # 小幅流入
            mask = (net_inflow_ratio >= 0) & (net_inflow_ratio <= 0.01)
            scores[mask] = 0.5 + net_inflow_ratio[mask] * 30  # 0.5-0.8

            # 大幅流入
            scores[net_inflow_ratio > 0.01] = 0.9

        elif 'fund_flow_ratio' in data.columns:
            # 使用资金流入比例
            flow_ratio = data['fund_flow_ratio']
            scores = pd.Series(index=data.index, dtype=float)

            scores[flow_ratio < -0.5] = 0.2
            scores[(flow_ratio >= -0.5) & (flow_ratio < 0)] = 0.4
            scores[(flow_ratio >= 0) & (flow_ratio <= 0.5)] = 0.6
            scores[flow_ratio > 0.5] = 0.9

        return scores.fillna(0.5)

    def get_name(self) -> str:
        return "资金面因子"

    def get_required_columns(self) -> list:
        return ['volume', 'amount']

    def get_description(self) -> str:
        return (
            f"资金面因子 - 权重: {self.weight:.2%}\n"
            "基于量比、成交额活跃度、换手率活跃度、主力资金流入等指标综合评分"
        )

    def get_factor_details(self) -> Dict[str, Any]:
        """获取因子详细信息"""
        return {
            'name': self.get_name(),
            'weight': self.weight,
            'components': {
                'volume_ratio': {'weight': 0.30, 'description': '量比，适度放量为佳'},
                'amount_activity': {'weight': 0.25, 'description': '成交额活跃度'},
                'turnover_activity': {'weight': 0.25, 'description': '换手率活跃度'},
                'fund_flow': {'weight': 0.20, 'description': '主力资金净流入'}
            },
            'score_range': [0.0, 1.0],
            'optimal_range': {
                'volume_ratio': [1.5, 3.0],
                'relative_turnover': [1.2, 2.5],
                'fund_flow_ratio': [0.0, 0.5]
            }
        }