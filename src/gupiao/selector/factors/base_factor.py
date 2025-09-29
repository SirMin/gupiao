"""
因子基类 - 定义所有评分因子的统一接口
"""
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Any, Dict
import logging


class BaseFactor(ABC):
    """评分因子基类

    定义所有评分因子的统一接口，包括因子计算、标准化和权重管理
    """

    def __init__(self, weight: float = 1.0, config: Dict[str, Any] = None):
        """
        初始化因子

        Args:
            weight: 因子权重 (0-1之间)
            config: 因子配置参数
        """
        self.weight = weight
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

        # 标准化方法
        self.normalization_method = config.get('normalization_method', 'min_max') if config else 'min_max'

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算因子值

        Args:
            data: 输入的股票数据DataFrame

        Returns:
            因子得分Series，索引为股票代码，值为0-1之间的标准化分数
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        获取因子名称

        Returns:
            因子名称字符串
        """
        pass

    def get_description(self) -> str:
        """
        获取因子描述

        Returns:
            因子描述字符串
        """
        return f"{self.get_name()} - 权重: {self.weight}"

    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        验证输入数据的完整性

        Args:
            data: 输入数据

        Returns:
            数据是否有效
        """
        if data.empty:
            self.logger.warning(f"{self.get_name()}: 输入数据为空")
            return False

        required_columns = self.get_required_columns()
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            self.logger.warning(f"{self.get_name()}: 缺少必需的列: {missing_columns}")
            return False

        return True

    def get_required_columns(self) -> list:
        """
        获取因子计算需要的列

        Returns:
            必需列名列表
        """
        return []

    def normalize(self, values: pd.Series) -> pd.Series:
        """
        标准化因子值到0-1区间

        Args:
            values: 原始因子值

        Returns:
            标准化后的因子值
        """
        if values.empty:
            return values

        # 去除无效值
        valid_values = values.dropna()
        if valid_values.empty:
            self.logger.warning(f"{self.get_name()}: 所有值都是无效的")
            return pd.Series(index=values.index, dtype=float)

        if self.normalization_method == 'min_max':
            return self._min_max_normalize(values)
        elif self.normalization_method == 'z_score':
            return self._z_score_normalize(values)
        elif self.normalization_method == 'rank':
            return self._rank_normalize(values)
        else:
            self.logger.warning(f"未知的标准化方法: {self.normalization_method}")
            return self._min_max_normalize(values)

    def _min_max_normalize(self, values: pd.Series) -> pd.Series:
        """Min-Max标准化到[0,1]区间"""
        min_val = values.min()
        max_val = values.max()

        if min_val == max_val:
            # 所有值相同，返回0.5
            return pd.Series(0.5, index=values.index)

        return (values - min_val) / (max_val - min_val)

    def _z_score_normalize(self, values: pd.Series) -> pd.Series:
        """Z-score标准化后映射到[0,1]区间"""
        mean_val = values.mean()
        std_val = values.std()

        if std_val == 0:
            return pd.Series(0.5, index=values.index)

        z_scores = (values - mean_val) / std_val

        # 使用sigmoid函数将z-score映射到[0,1]
        return 1 / (1 + np.exp(-z_scores))

    def _rank_normalize(self, values: pd.Series) -> pd.Series:
        """基于排名的标准化"""
        ranks = values.rank(method='dense')
        return (ranks - 1) / (ranks.max() - 1) if ranks.max() > 1 else pd.Series(0.5, index=values.index)

    def calculate_weighted_score(self, data: pd.DataFrame) -> pd.Series:
        """
        计算加权后的因子得分

        Args:
            data: 输入数据

        Returns:
            加权因子得分
        """
        if not self.validate_data(data):
            return pd.Series(dtype=float)

        try:
            raw_scores = self.calculate(data)
            normalized_scores = self.normalize(raw_scores)
            weighted_scores = normalized_scores * self.weight

            self.logger.debug(
                f"{self.get_name()}: 计算完成 "
                f"(样本数: {len(weighted_scores)}, "
                f"平均分: {weighted_scores.mean():.3f}, "
                f"权重: {self.weight})"
            )

            return weighted_scores

        except Exception as e:
            self.logger.error(f"{self.get_name()}: 计算失败 - {str(e)}")
            return pd.Series(dtype=float)

    def get_factor_statistics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        获取因子统计信息

        Args:
            data: 输入数据

        Returns:
            因子统计信息字典
        """
        if not self.validate_data(data):
            return {'error': '数据验证失败'}

        try:
            raw_scores = self.calculate(data)
            normalized_scores = self.normalize(raw_scores)

            stats = {
                'factor_name': self.get_name(),
                'weight': self.weight,
                'sample_count': len(raw_scores),
                'valid_count': raw_scores.count(),
                'raw_stats': {
                    'mean': raw_scores.mean(),
                    'std': raw_scores.std(),
                    'min': raw_scores.min(),
                    'max': raw_scores.max(),
                    'quantiles': raw_scores.quantile([0.25, 0.5, 0.75]).to_dict()
                },
                'normalized_stats': {
                    'mean': normalized_scores.mean(),
                    'std': normalized_scores.std(),
                    'min': normalized_scores.min(),
                    'max': normalized_scores.max()
                }
            }

            return stats

        except Exception as e:
            return {'error': str(e)}

    def set_weight(self, weight: float):
        """
        设置因子权重

        Args:
            weight: 新的权重值
        """
        if not 0 <= weight <= 1:
            self.logger.warning(f"权重值 {weight} 超出 [0,1] 范围")

        old_weight = self.weight
        self.weight = weight
        self.logger.info(f"{self.get_name()}: 权重更新 {old_weight} -> {weight}")

    def update_config(self, config: Dict[str, Any]):
        """
        更新因子配置

        Args:
            config: 新的配置参数
        """
        old_config = self.config.copy()
        self.config.update(config)
        self.logger.info(f"{self.get_name()}: 配置更新 {old_config} -> {self.config}")

    def handle_missing_values(self, values: pd.Series, method: str = 'median') -> pd.Series:
        """
        处理缺失值

        Args:
            values: 包含缺失值的Series
            method: 处理方法 ('mean', 'median', 'forward', 'zero', 'drop')

        Returns:
            处理后的Series
        """
        if values.isna().sum() == 0:
            return values

        if method == 'mean':
            return values.fillna(values.mean())
        elif method == 'median':
            return values.fillna(values.median())
        elif method == 'forward':
            return values.fillna(method='ffill')
        elif method == 'zero':
            return values.fillna(0)
        elif method == 'drop':
            return values.dropna()
        else:
            self.logger.warning(f"未知的缺失值处理方法: {method}")
            return values.fillna(values.median())

    def detect_outliers(self, values: pd.Series, method: str = 'iqr', threshold: float = 1.5) -> pd.Series:
        """
        检测异常值

        Args:
            values: 输入值
            method: 检测方法 ('iqr', 'z_score')
            threshold: 异常值阈值

        Returns:
            布尔Series，True表示异常值
        """
        if method == 'iqr':
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            return (values < lower_bound) | (values > upper_bound)

        elif method == 'z_score':
            z_scores = np.abs((values - values.mean()) / values.std())
            return z_scores > threshold

        else:
            self.logger.warning(f"未知的异常值检测方法: {method}")
            return pd.Series(False, index=values.index)