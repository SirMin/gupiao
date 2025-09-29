"""
财务因子 - 基于财务数据的评分因子
"""
import pandas as pd
import numpy as np
from typing import Dict, Any
from .base_factor import BaseFactor
import sys
import os

# 添加数据源路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../gupiao'))


class FinancialFactor(BaseFactor):
    """财务因子

    基于财务指标计算得分：
    - ROE (净资产收益率)
    - 营收增长率
    - 净利润同比增长率
    - 负债率 (越低越好)
    - 自由现金流

    财务数据需要从数据源获取，这里提供基础框架
    """

    def __init__(self, weight: float = 0.3, config: Dict[str, Any] = None):
        super().__init__(weight, config)
        self.data_source = None  # 需要注入数据源

    def set_data_source(self, data_source):
        """设置数据源"""
        self.data_source = data_source

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        计算财务因子得分

        Args:
            data: 股票基础数据

        Returns:
            财务因子得分Series
        """
        if data.empty:
            return pd.Series(dtype=float)

        # 初始化得分Series
        scores = pd.Series(index=data.index, dtype=float)

        try:
            # 如果有财务数据列，直接使用
            if self._has_financial_data(data):
                scores = self._calculate_from_existing_data(data)
            else:
                # 否则尝试从数据源获取财务数据
                scores = self._calculate_from_datasource(data)

            # 处理缺失值
            scores = self.handle_missing_values(scores, method='median')

            self.logger.info(f"财务因子计算完成，有效样本: {scores.count()}")

        except Exception as e:
            self.logger.error(f"财务因子计算失败: {str(e)}")
            # 返回默认分数
            scores = pd.Series(0.5, index=data.index)

        return scores

    def _has_financial_data(self, data: pd.DataFrame) -> bool:
        """检查是否已有财务数据"""
        financial_columns = ['roe', 'revenue_growth', 'profit_growth', 'debt_ratio', 'free_cash_flow']
        return any(col in data.columns for col in financial_columns)

    def _calculate_from_existing_data(self, data: pd.DataFrame) -> pd.Series:
        """基于现有财务数据计算得分"""
        scores = pd.Series(0.0, index=data.index)
        total_weight = 0.0

        # ROE得分 (权重: 25%)
        if 'roe' in data.columns:
            roe_scores = self._calculate_roe_score(data['roe'])
            scores += roe_scores * 0.25
            total_weight += 0.25

        # 营收增长率得分 (权重: 25%)
        if 'revenue_growth' in data.columns:
            revenue_scores = self._calculate_growth_score(data['revenue_growth'])
            scores += revenue_scores * 0.25
            total_weight += 0.25

        # 净利润增长率得分 (权重: 25%)
        if 'profit_growth' in data.columns:
            profit_scores = self._calculate_growth_score(data['profit_growth'])
            scores += profit_scores * 0.25
            total_weight += 0.25

        # 负债率得分 (权重: 15%, 越低越好)
        if 'debt_ratio' in data.columns:
            debt_scores = self._calculate_debt_score(data['debt_ratio'])
            scores += debt_scores * 0.15
            total_weight += 0.15

        # 自由现金流得分 (权重: 10%)
        if 'free_cash_flow' in data.columns:
            fcf_scores = self._calculate_fcf_score(data['free_cash_flow'])
            scores += fcf_scores * 0.10
            total_weight += 0.10

        # 标准化得分
        if total_weight > 0:
            scores = scores / total_weight
        else:
            scores = pd.Series(0.5, index=data.index)

        return scores

    def _calculate_from_datasource(self, data: pd.DataFrame) -> pd.Series:
        """从数据源获取财务数据并计算得分"""
        if self.data_source is None:
            self.logger.warning("未设置数据源，使用默认得分")
            return pd.Series(0.5, index=data.index)

        # 这里需要实现从数据源获取财务数据的逻辑
        # 由于baostock的财务数据获取比较复杂，这里提供框架
        scores = pd.Series(0.5, index=data.index)

        # 尝试为每只股票获取财务数据
        for idx, row in data.iterrows():
            code = row.get('code', '')
            if code:
                try:
                    financial_score = self._get_stock_financial_score(code)
                    scores.loc[idx] = financial_score
                except Exception as e:
                    self.logger.debug(f"获取 {code} 财务数据失败: {str(e)}")

        return scores

    def _get_stock_financial_score(self, code: str) -> float:
        """获取单只股票的财务得分"""
        # 这里应该调用数据源获取财务数据
        # 由于实现复杂，暂时返回模拟得分

        # 模拟基于股票代码的得分计算
        # 实际应用中应该调用数据源的财务数据接口
        hash_value = hash(code) % 100
        return 0.3 + (hash_value / 100) * 0.4  # 0.3-0.7之间的得分

    def _calculate_roe_score(self, roe: pd.Series) -> pd.Series:
        """计算ROE得分"""
        # ROE越高越好，但要考虑合理范围
        normalized_roe = roe.clip(0, 30) / 30  # 限制在0-30%范围内
        return normalized_roe.fillna(0.5)

    def _calculate_growth_score(self, growth: pd.Series) -> pd.Series:
        """计算增长率得分"""
        # 增长率在0-50%之间给高分，负增长给低分
        scores = pd.Series(index=growth.index, dtype=float)

        # 负增长
        scores[growth < 0] = 0.2

        # 0-10%增长
        mask = (growth >= 0) & (growth <= 0.1)
        scores[mask] = 0.5 + growth[mask] * 2  # 0.5-0.7

        # 10-30%增长
        mask = (growth > 0.1) & (growth <= 0.3)
        scores[mask] = 0.7 + (growth[mask] - 0.1) * 1.5  # 0.7-1.0

        # 超过30%的增长
        scores[growth > 0.3] = 1.0

        return scores.fillna(0.5)

    def _calculate_debt_score(self, debt_ratio: pd.Series) -> pd.Series:
        """计算负债率得分 (越低越好)"""
        # 负债率在0-100%之间，越低得分越高
        normalized_debt = debt_ratio.clip(0, 100) / 100
        scores = 1 - normalized_debt  # 反向计算
        return scores.fillna(0.5)

    def _calculate_fcf_score(self, fcf: pd.Series) -> pd.Series:
        """计算自由现金流得分"""
        # 正现金流给高分，负现金流给低分
        scores = pd.Series(index=fcf.index, dtype=float)

        # 负现金流
        scores[fcf < 0] = 0.2

        # 正现金流，使用对数变换避免极值影响
        positive_fcf = fcf[fcf >= 0]
        if not positive_fcf.empty:
            log_fcf = np.log1p(positive_fcf)  # log(1+x)
            normalized_log_fcf = log_fcf / log_fcf.max() if log_fcf.max() > 0 else log_fcf
            scores[fcf >= 0] = 0.5 + normalized_log_fcf * 0.5  # 0.5-1.0

        return scores.fillna(0.5)

    def get_name(self) -> str:
        return "财务因子"

    def get_required_columns(self) -> list:
        return []  # 不强制要求，因为可以从数据源获取

    def get_description(self) -> str:
        return (
            f"财务因子 - 权重: {self.weight:.2%}\n"
            "基于ROE、营收增长、净利润增长、负债率、自由现金流等指标综合评分"
        )

    def get_factor_details(self) -> Dict[str, Any]:
        """获取因子详细信息"""
        return {
            'name': self.get_name(),
            'weight': self.weight,
            'components': {
                'roe': {'weight': 0.25, 'description': '净资产收益率，越高越好'},
                'revenue_growth': {'weight': 0.25, 'description': '营收增长率，适度增长为佳'},
                'profit_growth': {'weight': 0.25, 'description': '净利润增长率，适度增长为佳'},
                'debt_ratio': {'weight': 0.15, 'description': '负债率，越低越好'},
                'free_cash_flow': {'weight': 0.10, 'description': '自由现金流，正值为佳'}
            },
            'score_range': [0.0, 1.0],
            'data_source': 'financial_statements'
        }