"""
CSV报告生成器 - 生成CSV格式的选股报告
"""
import pandas as pd
import os
from typing import Dict, Any, Optional
import logging
from datetime import datetime
from ..utils.helpers import ensure_directory_exists, format_number, format_percentage


class CSVReporter:
    """CSV报告生成器

    负责生成CSV格式的选股报告，包括：
    - 候选股票列表
    - 评分详情
    - 策略信息
    - 因子分析
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化CSV报告生成器

        Args:
            config: 输出配置
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        self.output_path = config.get('output_path', './results/')
        self.filename_template = config.get('filename_template', 'stock_candidates_{date}.{ext}')

    def generate_report(self, report_data: Dict[str, Any]) -> Optional[str]:
        """
        生成CSV报告

        Args:
            report_data: 报告数据

        Returns:
            生成的文件路径，失败时返回None
        """
        try:
            candidates = report_data.get('candidates', pd.DataFrame())
            if candidates.empty:
                self.logger.warning("无候选股票数据，跳过报告生成")
                return None

            target_date = report_data.get('target_date', datetime.now().strftime('%Y-%m-%d'))

            # 生成主报告文件
            main_file = self._generate_main_report(candidates, target_date, report_data)

            # 生成详细分析文件（可选）
            if self.config.get('include_detailed_analysis', False):
                self._generate_detailed_analysis(candidates, target_date, report_data)

            return main_file

        except Exception as e:
            self.logger.error(f"CSV报告生成失败: {str(e)}")
            return None

    def _generate_main_report(self, candidates: pd.DataFrame, target_date: str, report_data: Dict[str, Any]) -> str:
        """生成主要CSV报告"""
        # 生成文件路径
        filename = self.filename_template.format(date=target_date, ext='csv')
        file_path = os.path.join(self.output_path, filename)
        ensure_directory_exists(file_path)

        # 准备输出数据
        output_data = self._prepare_output_data(candidates, report_data)

        # 保存CSV文件
        output_data.to_csv(file_path, index=False, encoding='utf-8-sig')

        self.logger.info(f"主要CSV报告生成成功: {file_path}")
        return file_path

    def _prepare_output_data(self, candidates: pd.DataFrame, report_data: Dict[str, Any]) -> pd.DataFrame:
        """准备输出数据"""
        output_columns = [
            'code', 'name', 'close', 'total_score',
            'consecutive_up_days', 'return_3d', 'turnover',
            'market_value_billion', 'strategy_level', 'strategy_name'
        ]

        # 添加因子得分列
        factor_columns = []
        for col in candidates.columns:
            if col.endswith('_score') and col != 'total_score':
                factor_columns.append(col)

        output_columns.extend(factor_columns)

        # 选择存在的列
        available_columns = [col for col in output_columns if col in candidates.columns]
        output_data = candidates[available_columns].copy()

        # 格式化数值列
        if 'close' in output_data.columns:
            output_data['close'] = output_data['close'].round(2)

        if 'total_score' in output_data.columns:
            output_data['total_score'] = output_data['total_score'].round(4)

        if 'return_3d' in output_data.columns:
            output_data['return_3d'] = (output_data['return_3d'] * 100).round(2)  # 转换为百分比

        if 'turnover' in output_data.columns:
            output_data['turnover'] = (output_data['turnover'] * 100).round(2)  # 转换为百分比

        if 'market_value_billion' in output_data.columns:
            output_data['market_value_billion'] = output_data['market_value_billion'].round(2)

        # 格式化因子得分列
        for col in factor_columns:
            if col in output_data.columns:
                output_data[col] = output_data[col].round(4)

        # 重命名列为中文（可选）
        if self.config.get('use_chinese_headers', True):
            column_mapping = {
                'code': '股票代码',
                'name': '股票名称',
                'close': '收盘价',
                'total_score': '综合评分',
                'consecutive_up_days': '连续上涨天数',
                'return_3d': '3日收益率(%)',
                'turnover': '换手率(%)',
                'market_value_billion': '市值(亿)',
                'strategy_level': '策略级别',
                'strategy_name': '策略名称',
                'financial_score': '财务得分',
                'fund_flow_score': '资金面得分',
                'stability_score': '稳定性得分',
                'price_position_score': '价格位置得分'
            }

            output_data = output_data.rename(columns=column_mapping)

        return output_data

    def _generate_detailed_analysis(self, candidates: pd.DataFrame, target_date: str, report_data: Dict[str, Any]):
        """生成详细分析文件"""
        try:
            # 生成策略分析文件
            strategy_file = self._generate_strategy_analysis(report_data, target_date)

            # 生成因子分析文件
            factor_file = self._generate_factor_analysis(candidates, target_date)

            self.logger.info(f"详细分析文件生成完成: {strategy_file}, {factor_file}")

        except Exception as e:
            self.logger.error(f"详细分析文件生成失败: {str(e)}")

    def _generate_strategy_analysis(self, report_data: Dict[str, Any], target_date: str) -> str:
        """生成策略分析文件"""
        strategy_info = report_data.get('strategy_info', {})

        # 准备策略分析数据
        strategy_data = []

        # 添加策略执行结果
        strategy_summary = strategy_info.get('summary', {})
        strategy_results = strategy_summary.get('strategy_results', [])

        for result in strategy_results:
            strategy_data.append({
                '策略级别': result.get('level', ''),
                '策略名称': result.get('name', ''),
                '候选股票数': result.get('candidates_count', 0),
                '执行状态': '成功' if result.get('success', False) else '失败',
                '错误信息': result.get('error', '')
            })

        # 转换为DataFrame
        strategy_df = pd.DataFrame(strategy_data)

        # 保存文件
        filename = f'strategy_analysis_{target_date}.csv'
        file_path = os.path.join(self.output_path, filename)
        strategy_df.to_csv(file_path, index=False, encoding='utf-8-sig')

        return file_path

    def _generate_factor_analysis(self, candidates: pd.DataFrame, target_date: str) -> str:
        """生成因子分析文件"""
        factor_data = []

        # 分析各个因子得分
        factor_columns = [col for col in candidates.columns if col.endswith('_score')]

        for col in factor_columns:
            if col == 'total_score':
                continue

            factor_name = col.replace('_score', '')
            scores = candidates[col].dropna()

            if not scores.empty:
                factor_data.append({
                    '因子名称': factor_name,
                    '样本数': len(scores),
                    '平均分': scores.mean(),
                    '标准差': scores.std(),
                    '最小值': scores.min(),
                    '最大值': scores.max(),
                    '中位数': scores.median(),
                    '25分位数': scores.quantile(0.25),
                    '75分位数': scores.quantile(0.75),
                    '高分比例(>0.7)': (scores > 0.7).mean(),
                    '低分比例(<0.3)': (scores < 0.3).mean()
                })

        # 转换为DataFrame
        factor_df = pd.DataFrame(factor_data)

        # 格式化数值
        numeric_columns = ['平均分', '标准差', '最小值', '最大值', '中位数', '25分位数', '75分位数']
        for col in numeric_columns:
            if col in factor_df.columns:
                factor_df[col] = factor_df[col].round(4)

        percentage_columns = ['高分比例(>0.7)', '低分比例(<0.3)']
        for col in percentage_columns:
            if col in factor_df.columns:
                factor_df[col] = (factor_df[col] * 100).round(2)

        # 保存文件
        filename = f'factor_analysis_{target_date}.csv'
        file_path = os.path.join(self.output_path, filename)
        factor_df.to_csv(file_path, index=False, encoding='utf-8-sig')

        return file_path

    def generate_summary_report(self, batch_results: list, output_filename: str = None) -> Optional[str]:
        """
        生成批量结果汇总报告

        Args:
            batch_results: 批量处理结果列表
            output_filename: 输出文件名

        Returns:
            生成的文件路径
        """
        try:
            if not batch_results:
                self.logger.warning("无批量结果数据")
                return None

            summary_data = []

            for result in batch_results:
                if not result.get('success', False):
                    continue

                target_date = result.get('target_date', '')
                data_summary = result.get('data_summary', {})
                strategy_info = result.get('strategy_info', {})

                summary_data.append({
                    '日期': target_date,
                    '总股票数': data_summary.get('total_stocks', 0),
                    '有效股票数': data_summary.get('valid_stocks', 0),
                    '候选股票数': data_summary.get('candidate_stocks', 0),
                    '最终候选数': data_summary.get('final_candidates', 0),
                    '使用策略': strategy_info.get('strategy_used', ''),
                    '策略级别': strategy_info.get('strategy_level', ''),
                    '执行时间(秒)': result.get('execution_time', 0)
                })

            if not summary_data:
                self.logger.warning("无有效的批量结果数据")
                return None

            # 转换为DataFrame
            summary_df = pd.DataFrame(summary_data)

            # 格式化数值
            if '执行时间(秒)' in summary_df.columns:
                summary_df['执行时间(秒)'] = summary_df['执行时间(秒)'].round(2)

            # 保存文件
            if output_filename is None:
                output_filename = f'batch_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

            file_path = os.path.join(self.output_path, output_filename)
            ensure_directory_exists(file_path)
            summary_df.to_csv(file_path, index=False, encoding='utf-8-sig')

            self.logger.info(f"批量汇总报告生成成功: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"批量汇总报告生成失败: {str(e)}")
            return None

    def export_config_report(self, config_data: Dict[str, Any], target_date: str) -> Optional[str]:
        """
        导出配置报告

        Args:
            config_data: 配置数据
            target_date: 目标日期

        Returns:
            生成的文件路径
        """
        try:
            config_items = []

            def flatten_config(data: Dict[str, Any], prefix: str = ''):
                """递归展平配置字典"""
                for key, value in data.items():
                    full_key = f'{prefix}.{key}' if prefix else key

                    if isinstance(value, dict):
                        flatten_config(value, full_key)
                    else:
                        config_items.append({
                            '配置项': full_key,
                            '配置值': str(value),
                            '数据类型': type(value).__name__
                        })

            flatten_config(config_data)

            # 转换为DataFrame
            config_df = pd.DataFrame(config_items)

            # 保存文件
            filename = f'config_report_{target_date}.csv'
            file_path = os.path.join(self.output_path, filename)
            ensure_directory_exists(file_path)
            config_df.to_csv(file_path, index=False, encoding='utf-8-sig')

            self.logger.info(f"配置报告生成成功: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"配置报告生成失败: {str(e)}")
            return None