"""
评分引擎 - 管理多个评分因子的计算和权重合成
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import logging
import sys
import os

from ..factors.financial_factor import FinancialFactor
from ..factors.fund_flow_factor import FundFlowFactor
from ..factors.stability_factor import StabilityFactor
from ..factors.price_position_factor import PricePositionFactor


class ScoreEngine:
    """评分引擎

    管理多个评分因子的计算和权重合成：
    - 财务因子 (FinancialFactor)
    - 资金面因子 (FundFlowFactor)
    - 稳定性因子 (StabilityFactor)
    - 价格位置因子 (PricePositionFactor)

    支持动态权重调整和因子启用/禁用
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化评分引擎

        Args:
            config: 评分引擎配置
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化因子
        self.factors = {}
        self._initialize_factors()

        # 权重验证
        self._validate_weights()

    def _initialize_factors(self):
        """初始化所有因子"""
        factor_configs = self.config.get('factors', {})

        # 财务因子
        if factor_configs.get('financial', {}).get('enabled', True):
            weight = factor_configs.get('financial', {}).get('weight', 0.30)
            config = factor_configs.get('financial', {})
            self.factors['financial'] = FinancialFactor(weight, config)

        # 资金面因子
        if factor_configs.get('fund_flow', {}).get('enabled', True):
            weight = factor_configs.get('fund_flow', {}).get('weight', 0.25)
            config = factor_configs.get('fund_flow', {})
            self.factors['fund_flow'] = FundFlowFactor(weight, config)

        # 稳定性因子
        if factor_configs.get('stability', {}).get('enabled', True):
            weight = factor_configs.get('stability', {}).get('weight', 0.25)
            config = factor_configs.get('stability', {})
            self.factors['stability'] = StabilityFactor(weight, config)

        # 价格位置因子
        if factor_configs.get('price_position', {}).get('enabled', True):
            weight = factor_configs.get('price_position', {}).get('weight', 0.20)
            config = factor_configs.get('price_position', {})
            self.factors['price_position'] = PricePositionFactor(weight, config)

        self.logger.info(f"初始化完成，启用因子: {list(self.factors.keys())}")

    def _validate_weights(self):
        """验证权重配置"""
        if not self.factors:
            self.logger.warning("未启用任何因子")
            return

        total_weight = sum(factor.weight for factor in self.factors.values())

        if abs(total_weight - 1.0) > 0.01:  # 允许1%的误差
            self.logger.warning(f"因子权重总和为 {total_weight:.3f}，不等于1.0")

            # 自动标准化权重
            if total_weight > 0:
                for factor in self.factors.values():
                    factor.weight = factor.weight / total_weight
                self.logger.info("已自动标准化因子权重")

    def calculate_scores(self, candidates: pd.DataFrame) -> pd.DataFrame:
        """
        计算综合评分

        Args:
            candidates: 候选股票数据

        Returns:
            添加了各因子得分和综合得分的DataFrame
        """
        if candidates.empty:
            self.logger.warning("候选股票数据为空")
            return candidates

        self.logger.info(f"开始计算 {len(candidates)} 只股票的综合评分")

        result = candidates.copy()

        try:
            # 计算各个因子得分
            factor_scores = {}

            for factor_name, factor in self.factors.items():
                try:
                    self.logger.debug(f"计算 {factor_name} 因子得分")
                    scores = factor.calculate_weighted_score(result)

                    if not scores.empty:
                        # 添加因子得分列
                        score_column = f'{factor_name}_score'
                        result[score_column] = scores
                        factor_scores[factor_name] = scores

                        self.logger.debug(
                            f"{factor_name} 因子计算完成: "
                            f"平均分 {scores.mean():.3f}, "
                            f"权重 {factor.weight:.3f}"
                        )
                    else:
                        self.logger.warning(f"{factor_name} 因子返回空结果")

                except Exception as e:
                    self.logger.error(f"{factor_name} 因子计算失败: {str(e)}")
                    # 使用默认得分
                    score_column = f'{factor_name}_score'
                    result[score_column] = 0.5 * factor.weight

            # 计算加权综合分
            if factor_scores:
                total_scores = pd.Series(0.0, index=result.index)
                total_weight = 0.0

                for factor_name, scores in factor_scores.items():
                    total_scores += scores
                    total_weight += self.factors[factor_name].weight

                # 标准化综合得分到0-1区间
                if total_weight > 0:
                    result['total_score'] = total_scores / total_weight
                else:
                    result['total_score'] = 0.5

                # 按综合得分排序
                result = result.sort_values('total_score', ascending=False)

                self.logger.info(
                    f"综合评分计算完成: "
                    f"平均分 {result['total_score'].mean():.3f}, "
                    f"最高分 {result['total_score'].max():.3f}, "
                    f"最低分 {result['total_score'].min():.3f}"
                )

            else:
                self.logger.error("所有因子计算都失败")
                result['total_score'] = 0.5

        except Exception as e:
            self.logger.error(f"综合评分计算失败: {str(e)}")
            result['total_score'] = 0.5

        return result

    def get_factor_info(self) -> List[Dict[str, Any]]:
        """
        获取所有因子的信息

        Returns:
            因子信息列表
        """
        factor_info = []

        for factor_name, factor in self.factors.items():
            info = {
                'name': factor.get_name(),
                'factor_id': factor_name,
                'weight': factor.weight,
                'description': factor.get_description(),
                'required_columns': factor.get_required_columns()
            }

            # 如果因子有详细信息方法，添加详细信息
            if hasattr(factor, 'get_factor_details'):
                info['details'] = factor.get_factor_details()

            factor_info.append(info)

        return factor_info

    def analyze_factor_performance(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        分析各因子的表现

        Args:
            data: 包含因子得分的数据

        Returns:
            因子表现分析结果
        """
        if data.empty:
            return {'error': '数据为空'}

        analysis = {
            'sample_count': len(data),
            'factor_analysis': {}
        }

        for factor_name, factor in self.factors.items():
            score_column = f'{factor_name}_score'

            if score_column not in data.columns:
                continue

            scores = data[score_column]

            factor_analysis = {
                'factor_name': factor.get_name(),
                'weight': factor.weight,
                'statistics': {
                    'mean': scores.mean(),
                    'std': scores.std(),
                    'min': scores.min(),
                    'max': scores.max(),
                    'median': scores.median(),
                    'quantiles': {
                        '25%': scores.quantile(0.25),
                        '75%': scores.quantile(0.75)
                    }
                },
                'distribution': {
                    'high_score_ratio': (scores >= 0.7).mean(),  # 高分股票比例
                    'low_score_ratio': (scores <= 0.3).mean(),   # 低分股票比例
                    'zero_score_count': (scores == 0).sum()      # 零分股票数量
                }
            }

            # 如果因子有统计方法，添加详细统计
            if hasattr(factor, 'get_factor_statistics'):
                try:
                    detailed_stats = factor.get_factor_statistics(data)
                    factor_analysis['detailed_stats'] = detailed_stats
                except Exception as e:
                    self.logger.debug(f"获取 {factor_name} 详细统计失败: {str(e)}")

            analysis['factor_analysis'][factor_name] = factor_analysis

        # 综合得分分析
        if 'total_score' in data.columns:
            total_scores = data['total_score']
            analysis['total_score_analysis'] = {
                'statistics': {
                    'mean': total_scores.mean(),
                    'std': total_scores.std(),
                    'min': total_scores.min(),
                    'max': total_scores.max(),
                    'median': total_scores.median()
                },
                'distribution': {
                    'excellent_ratio': (total_scores >= 0.8).mean(),
                    'good_ratio': ((total_scores >= 0.6) & (total_scores < 0.8)).mean(),
                    'average_ratio': ((total_scores >= 0.4) & (total_scores < 0.6)).mean(),
                    'poor_ratio': (total_scores < 0.4).mean()
                }
            }

        return analysis

    def update_factor_weight(self, factor_name: str, new_weight: float):
        """
        更新因子权重

        Args:
            factor_name: 因子名称
            new_weight: 新权重
        """
        if factor_name not in self.factors:
            self.logger.error(f"因子 {factor_name} 不存在")
            return

        old_weight = self.factors[factor_name].weight
        self.factors[factor_name].set_weight(new_weight)

        self.logger.info(f"因子 {factor_name} 权重更新: {old_weight:.3f} -> {new_weight:.3f}")

        # 重新验证权重
        self._validate_weights()

    def enable_factor(self, factor_name: str):
        """启用因子"""
        if factor_name in self.factors:
            self.logger.info(f"因子 {factor_name} 已启用")
            return

        # 重新创建因子
        factor_configs = self.config.get('factors', {})
        if factor_name in factor_configs:
            config = factor_configs[factor_name]
            weight = config.get('weight', 0.1)

            if factor_name == 'financial':
                self.factors[factor_name] = FinancialFactor(weight, config)
            elif factor_name == 'fund_flow':
                self.factors[factor_name] = FundFlowFactor(weight, config)
            elif factor_name == 'stability':
                self.factors[factor_name] = StabilityFactor(weight, config)
            elif factor_name == 'price_position':
                self.factors[factor_name] = PricePositionFactor(weight, config)

            self.logger.info(f"已启用因子: {factor_name}")
            self._validate_weights()

    def disable_factor(self, factor_name: str):
        """禁用因子"""
        if factor_name in self.factors:
            del self.factors[factor_name]
            self.logger.info(f"已禁用因子: {factor_name}")
            self._validate_weights()

    def get_top_stocks(self, data: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        获取得分最高的N只股票

        Args:
            data: 包含评分的股票数据
            top_n: 返回股票数量

        Returns:
            Top N股票数据
        """
        if data.empty or 'total_score' not in data.columns:
            return pd.DataFrame()

        top_stocks = data.nlargest(top_n, 'total_score')

        self.logger.info(f"返回得分最高的 {len(top_stocks)} 只股票")

        return top_stocks

    def export_score_report(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        导出评分报告

        Args:
            data: 包含评分的股票数据

        Returns:
            评分报告字典
        """
        if data.empty:
            return {'error': '数据为空'}

        report = {
            'summary': {
                'total_stocks': len(data),
                'evaluation_time': pd.Timestamp.now().isoformat(),
                'enabled_factors': list(self.factors.keys()),
                'total_weight': sum(factor.weight for factor in self.factors.values())
            },
            'factor_performance': self.analyze_factor_performance(data),
            'top_stocks': self.get_top_stocks(data, 20).to_dict('records') if len(data) > 0 else [],
            'score_distribution': {
                'by_range': self._get_score_distribution(data),
                'percentiles': self._get_score_percentiles(data)
            }
        }

        return report

    def _get_score_distribution(self, data: pd.DataFrame) -> Dict[str, int]:
        """获取得分分布"""
        if 'total_score' not in data.columns:
            return {}

        total_scores = data['total_score']

        return {
            '0.9-1.0': ((total_scores >= 0.9) & (total_scores <= 1.0)).sum(),
            '0.8-0.9': ((total_scores >= 0.8) & (total_scores < 0.9)).sum(),
            '0.7-0.8': ((total_scores >= 0.7) & (total_scores < 0.8)).sum(),
            '0.6-0.7': ((total_scores >= 0.6) & (total_scores < 0.7)).sum(),
            '0.5-0.6': ((total_scores >= 0.5) & (total_scores < 0.6)).sum(),
            '0.4-0.5': ((total_scores >= 0.4) & (total_scores < 0.5)).sum(),
            '0.3-0.4': ((total_scores >= 0.3) & (total_scores < 0.4)).sum(),
            '0.2-0.3': ((total_scores >= 0.2) & (total_scores < 0.3)).sum(),
            '0.1-0.2': ((total_scores >= 0.1) & (total_scores < 0.2)).sum(),
            '0.0-0.1': ((total_scores >= 0.0) & (total_scores < 0.1)).sum()
        }

    def _get_score_percentiles(self, data: pd.DataFrame) -> Dict[str, float]:
        """获取得分分位数"""
        if 'total_score' not in data.columns:
            return {}

        total_scores = data['total_score']

        return {
            'p10': total_scores.quantile(0.1),
            'p25': total_scores.quantile(0.25),
            'p50': total_scores.quantile(0.5),
            'p75': total_scores.quantile(0.75),
            'p90': total_scores.quantile(0.9),
            'p95': total_scores.quantile(0.95),
            'p99': total_scores.quantile(0.99)
        }