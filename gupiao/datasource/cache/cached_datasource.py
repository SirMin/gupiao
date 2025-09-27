import os
import hashlib
from typing import Union, List, Optional, Dict, Any
from ..stock.base import StockDataSource, StockDataResult
from .metadata_manager import MetadataManager
from .file_cache import FileCache
from .range_calculator import DateRange, RangeCalculator
from .datasource_manager import DataSourceManager, LoadBalanceStrategy
from .query_engine import DuckDBQueryEngine


class CachedDataSource(StockDataSource):
    """缓存数据源主类"""

    def __init__(
        self,
        remote_datasources: Union[StockDataSource, List[StockDataSource]],
        cache_dir: str,
        compression: str = 'snappy',
        enable_duckdb: bool = True,
        duckdb_memory_limit: str = "2GB",
        duckdb_threads: int = 4,
        load_balance_strategy: str = "priority_first",
        failover_enabled: bool = True,
        circuit_breaker_enabled: bool = True,
        health_check_enabled: bool = True,
        **config
    ):
        """
        初始化缓存数据源

        Args:
            remote_datasources: 远程数据源，可以是单个数据源或数据源列表
            cache_dir: 缓存目录
            compression: 压缩算法
            enable_duckdb: 是否启用DuckDB查询引擎
            duckdb_memory_limit: DuckDB内存限制
            duckdb_threads: DuckDB线程数
            load_balance_strategy: 负载均衡策略
            failover_enabled: 是否启用故障转移
            circuit_breaker_enabled: 是否启用熔断器
            health_check_enabled: 是否启用健康检查
            **config: 其他配置参数
        """
        self.cache_dir = cache_dir
        self.compression = compression
        self.enable_duckdb = enable_duckdb

        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)

        # 初始化组件
        self.metadata_manager = MetadataManager(cache_dir)
        self.file_cache = FileCache(cache_dir, compression)

        # 处理远程数据源
        if isinstance(remote_datasources, list):
            datasources = remote_datasources
        else:
            datasources = [remote_datasources]

        # 初始化数据源管理器
        strategy_map = {
            "round_robin": LoadBalanceStrategy.ROUND_ROBIN,
            "weighted_round_robin": LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN,
            "priority_first": LoadBalanceStrategy.PRIORITY_FIRST,
            "random": LoadBalanceStrategy.RANDOM,
            "response_time": LoadBalanceStrategy.RESPONSE_TIME
        }

        self.datasource_manager = DataSourceManager(
            datasources=datasources,
            load_balance_strategy=strategy_map.get(load_balance_strategy, LoadBalanceStrategy.PRIORITY_FIRST),
            failover_enabled=failover_enabled,
            circuit_breaker_enabled=circuit_breaker_enabled,
            health_check_enabled=health_check_enabled,
            **config
        )

        # 初始化DuckDB查询引擎
        self.query_engine = None
        if enable_duckdb:
            try:
                self.query_engine = DuckDBQueryEngine(
                    cache_dir=cache_dir,
                    memory_limit=duckdb_memory_limit,
                    threads=duckdb_threads
                )
                # 注册缓存表
                self.query_engine.register_cache_tables()
            except ImportError:
                print("警告: 无法导入duckdb，SQL查询功能将不可用")
                self.query_engine = None

    # ==================== K线数据 ====================

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "d",
        adjustflag: str = "3"
    ) -> StockDataResult:
        """
        获取历史A股K线数据（支持缓存）

        Args:
            code: 股票代码
            fields: 指示简称
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据类型
            adjustflag: 复权类型

        Returns:
            StockDataResult: K线数据结果
        """
        # 参数验证
        if not self._validate_code(code):
            return StockDataResult(False, error_msg="股票代码格式错误")

        if start_date and not self._validate_date(start_date):
            return StockDataResult(False, error_msg="开始日期格式错误")

        if end_date and not self._validate_date(end_date):
            return StockDataResult(False, error_msg="结束日期格式错误")

        # 生成查询键
        query_key = self._generate_query_key(
            code=code,
            fields=fields,
            frequency=frequency,
            adjustflag=adjustflag
        )

        # 构造数据类型标识
        data_type = f"k_data_{frequency}_{adjustflag}"

        try:
            # 获取已缓存的范围
            cached_ranges = self.metadata_manager.get_cached_ranges(query_key)

            # 计算需要从远程获取的范围
            target_range = DateRange(start_date, end_date)
            missing_ranges = RangeCalculator.calculate_missing_ranges(
                start_date, end_date, cached_ranges
            )

            # 从缓存加载数据
            cached_data = None
            if cached_ranges:
                cached_data = self.file_cache.load_stock_data(
                    stock_code=code,
                    data_type=data_type,
                    date_range=target_range,
                    fields=fields.split(',')
                )

            # 从远程获取缺失数据
            remote_data_list = []
            for missing_range in missing_ranges:
                try:
                    remote_result = self.datasource_manager.execute_query(
                        'query_history_k_data_plus',
                        code=code,
                        fields=fields,
                        start_date=missing_range.start,
                        end_date=missing_range.end,
                        frequency=frequency,
                        adjustflag=adjustflag
                    )

                    if remote_result.success and remote_result.data:
                        remote_df = remote_result.to_dataframe()
                        if not remote_df.empty:
                            # 保存到缓存
                            self.file_cache.save_stock_data(
                                stock_code=code,
                                data_type=data_type,
                                data=remote_df,
                                date_range=missing_range
                            )

                            # 更新元数据
                            self.metadata_manager.update_cached_ranges(
                                query_key=query_key,
                                new_ranges=[missing_range],
                                fields=fields.split(',')
                            )

                            remote_data_list.append(remote_df)

                except Exception as e:
                    print(f"获取远程数据失败 {missing_range}: {e}")
                    continue

            # 合并数据
            all_data = []
            if cached_data is not None and not cached_data.empty:
                all_data.append(cached_data)

            all_data.extend(remote_data_list)

            if all_data:
                import pandas as pd
                final_data = pd.concat(all_data, ignore_index=True)

                # 去重并排序
                if 'date' in final_data.columns:
                    final_data = final_data.drop_duplicates(subset=['date'], keep='last')
                    final_data = final_data.sort_values('date')

                # 按目标范围过滤
                if 'date' in final_data.columns:
                    mask = (final_data['date'] >= start_date) & (final_data['date'] <= end_date)
                    final_data = final_data[mask]

                return StockDataResult(
                    success=True,
                    data=final_data,
                    fields=fields.split(',')
                )
            else:
                return StockDataResult(False, error_msg="未获取到任何数据")

        except Exception as e:
            return StockDataResult(False, error_msg=f"查询失败: {str(e)}")

    # ==================== 其他K线相关方法 ====================

    def query_dividend_data(self, code: str, year: str, yearType: str = "trade") -> StockDataResult:
        """查询除权除息信息"""
        return self.datasource_manager.execute_query(
            'query_dividend_data',
            code=code,
            year=year,
            yearType=yearType
        )

    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> StockDataResult:
        """查询复权因子信息"""
        return self.datasource_manager.execute_query(
            'query_adjust_factor',
            code=code,
            start_date=start_date,
            end_date=end_date
        )

    # ==================== 财务数据 ====================

    def query_profit_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频盈利能力数据"""
        return self._query_financial_data('profit_data', code, year, quarter)

    def query_operation_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频营运能力数据"""
        return self._query_financial_data('operation_data', code, year, quarter)

    def query_growth_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频成长能力数据"""
        return self._query_financial_data('growth_data', code, year, quarter)

    def query_balance_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频偿债能力数据"""
        return self._query_financial_data('balance_data', code, year, quarter)

    def query_cash_flow_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频现金流量数据"""
        return self._query_financial_data('cash_flow_data', code, year, quarter)

    def query_dupont_data(self, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """查询季频杜邦指数数据"""
        return self._query_financial_data('dupont_data', code, year, quarter)

    def query_performance_express_report(self, code: str, start_date: str, end_date: str) -> StockDataResult:
        """查询季频公司业绩快报数据"""
        return self.datasource_manager.execute_query(
            'query_performance_express_report',
            code=code,
            start_date=start_date,
            end_date=end_date
        )

    def query_forecast_report(self, code: str, start_date: str, end_date: str) -> StockDataResult:
        """查询季频公司业绩预告数据"""
        return self.datasource_manager.execute_query(
            'query_forecast_report',
            code=code,
            start_date=start_date,
            end_date=end_date
        )

    # ==================== 证券信息 ====================

    def query_trade_dates(self, start_date: str, end_date: str) -> StockDataResult:
        """查询交易日信息"""
        return self.datasource_manager.execute_query(
            'query_trade_dates',
            start_date=start_date,
            end_date=end_date
        )

    def query_all_stock(self, day: Optional[str] = None) -> StockDataResult:
        """查询所有股票代码"""
        return self.datasource_manager.execute_query(
            'query_all_stock',
            day=day
        )

    def query_stock_basic(self, code: str, fields: Optional[str] = None) -> StockDataResult:
        """查询证券基本资料"""
        return self.datasource_manager.execute_query(
            'query_stock_basic',
            code=code,
            fields=fields
        )

    # ==================== 宏观经济数据 ====================

    def query_deposit_rate_data(self, start_date: str, end_date: str) -> StockDataResult:
        """查询存款利率数据"""
        return self.datasource_manager.execute_query(
            'query_deposit_rate_data',
            start_date=start_date,
            end_date=end_date
        )

    def query_loan_rate_data(self, start_date: str, end_date: str) -> StockDataResult:
        """查询贷款利率数据"""
        return self.datasource_manager.execute_query(
            'query_loan_rate_data',
            start_date=start_date,
            end_date=end_date
        )

    def query_required_reserve_ratio_data(self, start_date: str, end_date: str) -> StockDataResult:
        """查询存款准备金率数据"""
        return self.datasource_manager.execute_query(
            'query_required_reserve_ratio_data',
            start_date=start_date,
            end_date=end_date
        )

    def query_money_supply_data_month(self, start_date: str, end_date: str) -> StockDataResult:
        """查询月度货币供应量数据"""
        return self.datasource_manager.execute_query(
            'query_money_supply_data_month',
            start_date=start_date,
            end_date=end_date
        )

    def query_money_supply_data_year(self, start_date: str, end_date: str) -> StockDataResult:
        """查询年度货币供应量数据"""
        return self.datasource_manager.execute_query(
            'query_money_supply_data_year',
            start_date=start_date,
            end_date=end_date
        )

    def query_shibor_data(self, start_date: str, end_date: str) -> StockDataResult:
        """查询银行间同业拆放利率数据"""
        return self.datasource_manager.execute_query(
            'query_shibor_data',
            start_date=start_date,
            end_date=end_date
        )

    # ==================== 板块数据 ====================

    def query_stock_industry(self, code: str, date: str) -> StockDataResult:
        """查询行业分类数据"""
        return self.datasource_manager.execute_query(
            'query_stock_industry',
            code=code,
            date=date
        )

    def query_sz50_stocks(self, date: str) -> StockDataResult:
        """查询上证50成分股数据"""
        return self.datasource_manager.execute_query(
            'query_sz50_stocks',
            date=date
        )

    def query_hs300_stocks(self, date: str) -> StockDataResult:
        """查询沪深300成分股数据"""
        return self.datasource_manager.execute_query(
            'query_hs300_stocks',
            date=date
        )

    def query_zz500_stocks(self, date: str) -> StockDataResult:
        """查询中证500成分股数据"""
        return self.datasource_manager.execute_query(
            'query_zz500_stocks',
            date=date
        )

    # ==================== 扩展功能 ====================

    def query_with_sql(self, sql: str, **params) -> StockDataResult:
        """
        使用SQL查询缓存数据

        Args:
            sql: SQL查询语句
            **params: 查询参数

        Returns:
            StockDataResult: 查询结果
        """
        if not self.query_engine:
            return StockDataResult(False, error_msg="DuckDB查询引擎未启用")

        try:
            result_df = self.query_engine.execute_sql(sql, **params)
            return StockDataResult(
                success=True,
                data=result_df,
                fields=result_df.columns.tolist()
            )
        except Exception as e:
            return StockDataResult(False, error_msg=f"SQL查询失败: {str(e)}")

    def add_datasource(self, datasource: StockDataSource, priority: int = 0) -> None:
        """
        添加数据源

        Args:
            datasource: 数据源实例
            priority: 优先级
        """
        self.datasource_manager.add_datasource(datasource, priority)

    def remove_datasource(self, datasource: StockDataSource) -> None:
        """
        移除数据源

        Args:
            datasource: 数据源实例
        """
        self.datasource_manager.remove_datasource(datasource)

    def get_datasource_status(self) -> Dict[str, Any]:
        """
        获取数据源状态

        Returns:
            Dict[str, Any]: 数据源状态信息
        """
        return self.datasource_manager.get_statistics()

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        # 获取文件缓存信息
        cache_info = self.file_cache.get_cache_info()

        # 获取元数据统计
        metadata_stats = self.metadata_manager.get_statistics()

        # 合并统计信息
        total_size_mb = cache_info['total_size_bytes'] / (1024 * 1024)

        stats = {
            'cache_dir': self.cache_dir,
            'total_size_mb': round(total_size_mb, 2),
            'total_files': cache_info['total_files'],
            'total_query_keys': metadata_stats['total_query_keys'],
            'total_cached_ranges': metadata_stats['total_cached_ranges'],
            'total_records': metadata_stats['total_records'],
            'date_coverage': metadata_stats['date_coverage'],
            'hit_rate': self._calculate_hit_rate(),
            'by_stock': cache_info['by_stock'],
            'by_date': cache_info['by_date']
        }

        # 添加DuckDB统计信息
        if self.query_engine:
            try:
                duckdb_stats = self.query_engine.get_query_statistics()
                stats['duckdb'] = duckdb_stats
            except Exception:
                pass

        return stats

    def cleanup_cache(self, retention_days: int = 365) -> Dict[str, int]:
        """
        清理缓存

        Args:
            retention_days: 保留天数

        Returns:
            Dict[str, int]: 清理统计信息
        """
        # 清理文件
        file_stats = self.file_cache.cleanup_old_files(retention_days)

        # 清理元数据
        metadata_count = self.metadata_manager.cleanup_expired_metadata(retention_days)

        return {
            'deleted_files': file_stats['deleted_files'],
            'freed_bytes': file_stats['freed_bytes'],
            'cleaned_metadata_keys': metadata_count
        }

    def optimize_storage(self) -> Dict[str, int]:
        """
        优化存储

        Returns:
            Dict[str, int]: 优化统计信息
        """
        # 文件存储优化
        file_stats = self.file_cache.optimize_storage()

        # 元数据修复
        metadata_stats = self.metadata_manager.repair_metadata()

        return {
            **file_stats,
            **metadata_stats
        }

    # ==================== 私有方法 ====================

    def _query_financial_data(self, data_type: str, code: str, year: Union[str, int], quarter: Optional[Union[str, int]] = None) -> StockDataResult:
        """
        查询财务数据的通用方法

        Args:
            data_type: 数据类型
            code: 股票代码
            year: 年份
            quarter: 季度

        Returns:
            StockDataResult: 财务数据结果
        """
        # 这里可以实现财务数据的缓存逻辑
        # 目前直接调用远程数据源
        method_map = {
            'profit_data': 'query_profit_data',
            'operation_data': 'query_operation_data',
            'growth_data': 'query_growth_data',
            'balance_data': 'query_balance_data',
            'cash_flow_data': 'query_cash_flow_data',
            'dupont_data': 'query_dupont_data'
        }

        method_name = method_map.get(data_type)
        if not method_name:
            return StockDataResult(False, error_msg=f"不支持的财务数据类型: {data_type}")

        return self.datasource_manager.execute_query(
            method_name,
            code=code,
            year=year,
            quarter=quarter
        )

    def _generate_query_key(self, **kwargs) -> str:
        """
        生成查询键

        Args:
            **kwargs: 查询参数

        Returns:
            str: 查询键
        """
        # 将参数转换为字符串并排序
        key_parts = []
        for key, value in sorted(kwargs.items()):
            if isinstance(value, list):
                value = ','.join(map(str, value))
            key_parts.append(f"{key}={value}")

        query_string = '&'.join(key_parts)

        # 生成哈希值
        return hashlib.md5(query_string.encode('utf-8')).hexdigest()[:16]

    def _calculate_hit_rate(self) -> float:
        """
        计算缓存命中率

        Returns:
            float: 命中率
        """
        # 这里是一个简化的实现
        # 实际应该基于查询统计来计算
        metadata_stats = self.metadata_manager.get_statistics()
        if metadata_stats['total_query_keys'] > 0:
            return min(metadata_stats['total_cached_ranges'] / metadata_stats['total_query_keys'], 1.0)
        return 0.0

    def __del__(self):
        """析构函数，清理资源"""
        try:
            if hasattr(self, 'query_engine') and self.query_engine:
                self.query_engine.cleanup()
        except Exception:
            pass