"""
主运行器 - 协调整个选股流程的执行
"""
import pandas as pd
from typing import Dict, Any, Optional
import logging
from datetime import datetime
import os

# 导入数据源模块
from gupiao.datasource.baostock.datasource import BaostockDataSource
from gupiao.datasource.cache.cached_datasource import CachedDataSource

# 导入核心模块
from .preprocessor import StockDataPreprocessor
from .strategy_engine import StrategyEngine
from .score_engine import ScoreEngine
from ..utils.config import ConfigManager
from ..utils.logger import setup_logging
from ..utils.helpers import ensure_directory_exists, validate_date_format
from ..reporters.csv_reporter import CSVReporter


class StockSelectorRunner:
    """股票选择器主运行器

    协调整个选股流程：
    1. 数据获取和预处理
    2. 策略筛选
    3. 评分排序
    4. 结果输出
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化运行器

        Args:
            config_path: 配置文件路径
        """
        # 加载配置
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.config

        # 设置日志
        self.logger_manager = setup_logging(self.config.get('logging', {}))
        self.logger = logging.getLogger(__name__)

        # 初始化组件
        self.data_source = None
        self.preprocessor = None
        self.strategy_engine = None
        self.score_engine = None
        self.reporter = None

        # 运行状态
        self.is_initialized = False
        self.last_run_result = None

        self.logger.info("股票选择器运行器初始化完成")

    def initialize(self) -> bool:
        """
        初始化所有组件

        Returns:
            是否初始化成功
        """
        try:
            self.logger.info("开始初始化组件...")

            # 初始化数据源
            baostockDataSource = BaostockDataSource()
            self.data_source = CachedDataSource(remote_datasources=baostockDataSource, cache_dir='./cache')
            # self.data_source = baostockDataSource
            self.logger.info("数据源初始化成功")

            # 初始化预处理器
            self.preprocessor = StockDataPreprocessor(self.data_source)
            self.logger.info("数据预处理器初始化成功")

            # 初始化策略引擎
            strategy_config = {
                'main_strategy': self.config.get('main_strategy', {}),
                'fallback_strategy1': self.config.get('fallback_strategy1', {}),
                'fallback_strategy2': self.config.get('fallback_strategy2', {}),
                'emergency_strategy': self.config.get('emergency_strategy', {})
            }
            # 合并基础过滤配置到各策略
            basic_filter = self.config.get('basic_filter', {})
            for strategy_name in strategy_config:
                strategy_config[strategy_name].update(basic_filter)

            self.strategy_engine = StrategyEngine(strategy_config)
            self.logger.info("策略引擎初始化成功")

            # 初始化评分引擎
            self.score_engine = ScoreEngine(self.config.get('score_engine', {}))
            self.logger.info("评分引擎初始化成功")

            # 初始化报告器
            self.reporter = CSVReporter(self.config.get('output', {}))
            self.logger.info("报告器初始化成功")

            self.is_initialized = True
            self.logger.info("所有组件初始化完成")
            return True

        except Exception as e:
            self.logger.error(f"组件初始化失败: {str(e)}")
            return False

    def run_stock_selection(self, target_date: str, stock_codes: Optional[list] = None) -> Dict[str, Any]:
        """
        执行选股流程

        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            stock_codes: 指定股票代码列表，如果为None则处理所有A股

        Returns:
            选股结果字典
        """
        if not self.is_initialized:
            if not self.initialize():
                return {'success': False, 'error': '组件初始化失败'}

        if not validate_date_format(target_date):
            return {'success': False, 'error': f'日期格式无效: {target_date}'}

        self.logger.info(f"开始执行选股流程，目标日期: {target_date}")

        start_time = datetime.now()

        try:
            # 第1步：获取和预处理数据
            self.logger.info("第1步：获取股票数据...")
            raw_data = self.preprocessor.get_stock_pool_data(target_date, stock_codes)

            if raw_data.empty:
                return {
                    'success': False,
                    'error': '未获取到股票数据',
                    'target_date': target_date
                }

            self.logger.info(f"获取到 {len(raw_data)} 只股票的数据")

            # 过滤有效股票
            valid_data = self.preprocessor.filter_valid_stocks(raw_data)
            self.logger.info(f"有效股票数: {len(valid_data)}")

            if valid_data.empty:
                return {
                    'success': False,
                    'error': '过滤后无有效股票数据',
                    'target_date': target_date
                }

            # 估算市值
            valid_data = self.preprocessor.estimate_market_value(valid_data)

            # 第2步：策略筛选
            self.logger.info("第2步：执行策略筛选...")
            strategy_result = self.strategy_engine.filter_stocks(valid_data)

            if strategy_result['candidates'].empty:
                return {
                    'success': False,
                    'error': '策略筛选后无候选股票',
                    'target_date': target_date,
                    'strategy_summary': strategy_result['summary']
                }

            candidates = strategy_result['candidates']
            self.logger.info(
                f"策略筛选完成，使用策略: {strategy_result['strategy_used']}, "
                f"候选股票: {len(candidates)} 只"
            )

            # 第3步：评分排序
            self.logger.info("第3步：计算综合评分...")
            scored_candidates = self.score_engine.calculate_scores(candidates)

            if scored_candidates.empty:
                return {
                    'success': False,
                    'error': '评分计算失败',
                    'target_date': target_date
                }

            # 获取最终候选股票
            max_candidates = self.config.get('output', {}).get('max_candidates', 20)
            final_candidates = scored_candidates.head(max_candidates)

            # 第4步：生成报告
            self.logger.info("第4步：生成选股报告...")
            report_result = self._generate_report(final_candidates, strategy_result, target_date)

            # 计算运行时间
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            # 构建结果
            result = {
                'success': True,
                'target_date': target_date,
                'execution_time': execution_time,
                'data_summary': {
                    'total_stocks': len(raw_data),
                    'valid_stocks': len(valid_data),
                    'candidate_stocks': len(candidates),
                    'final_candidates': len(final_candidates)
                },
                'strategy_info': {
                    'strategy_used': strategy_result['strategy_used'],
                    'strategy_level': strategy_result['strategy_level'],
                    'strategy_summary': strategy_result['summary']
                },
                'candidates': final_candidates.to_dict('records'),
                'report_files': report_result.get('files', []),
                'factor_analysis': self.score_engine.analyze_factor_performance(final_candidates)
            }

            self.last_run_result = result
            self.logger.info(f"选股流程完成，耗时: {execution_time:.2f}秒")

            return result

        except Exception as e:
            self.logger.error(f"选股流程执行失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'target_date': target_date
            }

    def _generate_report(self, candidates: pd.DataFrame, strategy_result: Dict[str, Any], target_date: str) -> Dict[str, Any]:
        """
        生成选股报告

        Args:
            candidates: 候选股票数据
            strategy_result: 策略结果
            target_date: 目标日期

        Returns:
            报告生成结果
        """
        try:
            # 准备报告数据
            report_data = {
                'candidates': candidates,
                'target_date': target_date,
                'strategy_info': strategy_result,
                'factor_info': self.score_engine.get_factor_info(),
                'config_summary': self._get_config_summary()
            }

            # 生成报告文件
            files_generated = []

            # CSV报告
            if 'csv' in self.config.get('output', {}).get('formats', ['csv']):
                csv_file = self.reporter.generate_report(report_data)
                if csv_file:
                    files_generated.append(csv_file)

            # JSON报告（可选）
            if 'json' in self.config.get('output', {}).get('formats', []):
                json_file = self._generate_json_report(report_data)
                if json_file:
                    files_generated.append(json_file)

            return {'success': True, 'files': files_generated}

        except Exception as e:
            self.logger.error(f"报告生成失败: {str(e)}")
            return {'success': False, 'error': str(e)}

    def _generate_json_report(self, report_data: Dict[str, Any]) -> Optional[str]:
        """生成JSON格式报告"""
        try:
            import json
            from datetime import datetime

            output_config = self.config.get('output', {})
            output_path = output_config.get('output_path', './results/')
            filename_template = output_config.get('filename_template', 'stock_candidates_{date}.{ext}')

            filename = filename_template.format(
                date=report_data['target_date'],
                ext='json'
            )
            file_path = os.path.join(output_path, filename)

            ensure_directory_exists(file_path)

            # 准备JSON数据
            json_data = {
                'meta': {
                    'target_date': report_data['target_date'],
                    'generated_at': datetime.now().isoformat(),
                    'strategy_used': report_data['strategy_info']['strategy_used'],
                    'candidate_count': len(report_data['candidates'])
                },
                'candidates': report_data['candidates'].to_dict('records'),
                'strategy_summary': report_data['strategy_info']['summary'],
                'factor_info': report_data['factor_info']
            }

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"JSON报告生成成功: {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"JSON报告生成失败: {str(e)}")
            return None

    def _get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        return {
            'basic_filter': self.config.get('basic_filter', {}),
            'main_strategy': self.config.get('main_strategy', {}),
            'score_engine': {
                'factors': {name: {'weight': info.get('weight', 0), 'enabled': info.get('enabled', True)}
                           for name, info in self.config.get('score_engine', {}).get('factors', {}).items()}
            },
            'output': self.config.get('output', {})
        }

    def run_batch_selection(self, date_list: list, stock_codes: Optional[list] = None) -> Dict[str, Any]:
        """
        批量执行选股

        Args:
            date_list: 日期列表
            stock_codes: 股票代码列表

        Returns:
            批量执行结果
        """
        if not self.is_initialized:
            if not self.initialize():
                return {'success': False, 'error': '组件初始化失败'}

        self.logger.info(f"开始批量选股，日期数: {len(date_list)}")

        results = []
        errors = []

        for i, date in enumerate(date_list):
            self.logger.info(f"处理日期 {i+1}/{len(date_list)}: {date}")

            try:
                result = self.run_stock_selection(date, stock_codes)
                if result['success']:
                    results.append(result)
                else:
                    errors.append({'date': date, 'error': result.get('error', '未知错误')})

            except Exception as e:
                self.logger.error(f"处理日期 {date} 时出错: {str(e)}")
                errors.append({'date': date, 'error': str(e)})

        return {
            'success': len(results) > 0,
            'total_dates': len(date_list),
            'successful_dates': len(results),
            'failed_dates': len(errors),
            'results': results,
            'errors': errors
        }

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态

        Returns:
            系统状态信息
        """
        return {
            'initialized': self.is_initialized,
            'components': {
                'data_source': self.data_source is not None and getattr(self.data_source, 'is_logged_in', False),
                'preprocessor': self.preprocessor is not None,
                'strategy_engine': self.strategy_engine is not None,
                'score_engine': self.score_engine is not None,
                'reporter': self.reporter is not None
            },
            'config_loaded': self.config is not None,
            'last_run': self.last_run_result is not None,
            'logger_status': self.logger_manager.get_log_status() if self.logger_manager else None
        }

    def cleanup(self):
        """清理资源"""
        try:
            if self.data_source and hasattr(self.data_source, 'logout'):
                self.data_source.logout()
            self.logger.info("资源清理完成")
        except Exception as e:
            self.logger.error(f"资源清理失败: {str(e)}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        # 参数用于异常处理，这里不需要使用
        _ = exc_type, exc_val, exc_tb
        self.cleanup()