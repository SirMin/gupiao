"""
策略引擎 - 管理多级回退策略的执行
"""
import pandas as pd
from typing import Dict, Any, List
import logging
import sys
import os

# 添加策略模块路径
sys.path.append(os.path.dirname(__file__))
from ..strategies.main_strategy import MainStrategy
from ..strategies.fallback_strategies import FallbackStrategy1, FallbackStrategy2, EmergencyStrategy


class StrategyEngine:
    """策略引擎

    管理多级策略的执行顺序：
    1. 主策略 - 最严格的筛选条件
    2. 回退策略1 - 轻微放宽条件
    3. 回退策略2 - 进一步放宽条件
    4. 兜底策略 - 保证非空的最宽松条件

    一旦某个策略找到候选股票，即停止执行后续策略
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化策略引擎

        Args:
            config: 策略配置参数
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化所有策略
        self.strategies = [
            MainStrategy(config.get('main_strategy', {})),
            FallbackStrategy1(config.get('fallback_strategy1', {})),
            FallbackStrategy2(config.get('fallback_strategy2', {})),
            EmergencyStrategy(config.get('emergency_strategy', {}))
        ]

        # 验证策略配置
        self._validate_strategies()

    def _validate_strategies(self):
        """验证所有策略的配置"""
        for i, strategy in enumerate(self.strategies):
            if not strategy.validate_config():
                self.logger.error(f"策略 {i} ({strategy.get_name()}) 配置验证失败")

    def filter_stocks(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        执行分级筛选策略

        Args:
            data: 输入的股票数据DataFrame

        Returns:
            包含筛选结果和元信息的字典
        """
        if data.empty:
            self.logger.warning("输入数据为空")
            return {
                'candidates': pd.DataFrame(),
                'strategy_used': None,
                'strategy_level': -1,
                'summary': {'total_input': 0, 'final_candidates': 0}
            }

        total_input = len(data)
        self.logger.info(f"开始策略筛选，输入股票数: {total_input}")

        # 记录每个策略的执行结果
        strategy_results = []

        # 按顺序执行策略
        for level, strategy in enumerate(self.strategies):
            self.logger.info(f"执行策略 {level}: {strategy.get_name()}")

            try:
                candidates = strategy.apply(data)

                # 记录策略结果
                strategy_result = {
                    'level': level,
                    'name': strategy.get_name(),
                    'candidates_count': len(candidates),
                    'success': True
                }

                if hasattr(strategy, 'get_filter_summary'):
                    strategy_result['summary'] = strategy.get_filter_summary(candidates)

                strategy_results.append(strategy_result)

                # 如果找到候选股票，停止执行后续策略
                if not candidates.empty:
                    self.logger.info(
                        f"策略 {level} ({strategy.get_name()}) "
                        f"找到 {len(candidates)} 只候选股票，停止回退"
                    )

                    return {
                        'candidates': candidates,
                        'strategy_used': strategy.get_name(),
                        'strategy_level': level,
                        'summary': {
                            'total_input': total_input,
                            'final_candidates': len(candidates),
                            'strategy_results': strategy_results
                        }
                    }

            except Exception as e:
                self.logger.error(f"策略 {level} ({strategy.get_name()}) 执行失败: {str(e)}")
                strategy_results.append({
                    'level': level,
                    'name': strategy.get_name(),
                    'candidates_count': 0,
                    'success': False,
                    'error': str(e)
                })

        # 所有策略都没有找到候选股票（理论上不应该发生，因为兜底策略应该保证非空）
        self.logger.warning("所有策略都未找到候选股票")
        return {
            'candidates': pd.DataFrame(),
            'strategy_used': None,
            'strategy_level': -1,
            'summary': {
                'total_input': total_input,
                'final_candidates': 0,
                'strategy_results': strategy_results
            }
        }

    def get_strategy_info(self) -> List[Dict[str, Any]]:
        """
        获取所有策略的信息

        Returns:
            策略信息列表
        """
        strategy_info = []
        for level, strategy in enumerate(self.strategies):
            info = {
                'level': level,
                'name': strategy.get_name(),
                'description': strategy.get_description(),
                'config': getattr(strategy, 'config', {})
            }
            strategy_info.append(info)

        return strategy_info

    def test_strategy(self, data: pd.DataFrame, strategy_level: int = 0) -> pd.DataFrame:
        """
        测试指定策略

        Args:
            data: 输入数据
            strategy_level: 策略级别 (0-3)

        Returns:
            策略筛选结果
        """
        if strategy_level < 0 or strategy_level >= len(self.strategies):
            self.logger.error(f"无效的策略级别: {strategy_level}")
            return pd.DataFrame()

        strategy = self.strategies[strategy_level]
        self.logger.info(f"测试策略 {strategy_level}: {strategy.get_name()}")

        try:
            return strategy.apply(data)
        except Exception as e:
            self.logger.error(f"策略测试失败: {str(e)}")
            return pd.DataFrame()

    def analyze_filter_effectiveness(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        分析各个策略的筛选效果

        Args:
            data: 输入数据

        Returns:
            筛选效果分析结果
        """
        if data.empty:
            return {'error': '输入数据为空'}

        analysis = {
            'total_input': len(data),
            'strategy_analysis': []
        }

        for level, strategy in enumerate(self.strategies):
            try:
                candidates = strategy.apply(data)
                reduction_rate = (len(data) - len(candidates)) / len(data) if len(data) > 0 else 0

                strategy_analysis = {
                    'level': level,
                    'name': strategy.get_name(),
                    'candidates_count': len(candidates),
                    'reduction_rate': reduction_rate,
                    'pass_rate': len(candidates) / len(data) if len(data) > 0 else 0
                }

                if hasattr(strategy, 'get_filter_summary') and not candidates.empty:
                    strategy_analysis['filter_summary'] = strategy.get_filter_summary(candidates)

                analysis['strategy_analysis'].append(strategy_analysis)

            except Exception as e:
                analysis['strategy_analysis'].append({
                    'level': level,
                    'name': strategy.get_name(),
                    'error': str(e)
                })

        return analysis

    def update_strategy_config(self, strategy_level: int, new_config: Dict[str, Any]):
        """
        更新指定策略的配置

        Args:
            strategy_level: 策略级别
            new_config: 新的配置参数
        """
        if strategy_level < 0 or strategy_level >= len(self.strategies):
            self.logger.error(f"无效的策略级别: {strategy_level}")
            return

        strategy = self.strategies[strategy_level]
        old_config = strategy.config.copy()
        strategy.config.update(new_config)

        self.logger.info(
            f"更新策略 {strategy_level} ({strategy.get_name()}) 配置: "
            f"{old_config} -> {strategy.config}"
        )

        # 重新验证配置
        if not strategy.validate_config():
            self.logger.warning(f"策略 {strategy_level} 新配置验证失败")

    def get_recommended_config(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        基于输入数据推荐策略配置

        Args:
            data: 样本数据

        Returns:
            推荐的配置参数
        """
        if data.empty:
            return {}

        recommendations = {}

        # 基于数据统计推荐基础过滤参数
        if 'close' in data.columns:
            price_quantiles = data['close'].quantile([0.1, 0.9])
            recommendations['max_price'] = min(100, price_quantiles[0.9] * 1.2)

        if 'market_value_billion' in data.columns:
            mv_quantiles = data['market_value_billion'].quantile([0.2, 0.8])
            recommendations['min_market_value'] = max(50, mv_quantiles[0.2] * 0.8) * 1e8
            recommendations['max_market_value'] = min(500, mv_quantiles[0.8] * 1.2) * 1e8

        if 'turnover' in data.columns:
            turnover_stats = data['turnover'].describe()
            recommendations['min_turnover'] = max(0.01, turnover_stats['25%'])
            recommendations['max_turnover'] = min(0.15, turnover_stats['75%'])

        self.logger.info(f"基于数据推荐的配置参数: {recommendations}")
        return recommendations