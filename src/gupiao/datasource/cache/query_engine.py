import os
import re
import threading
from typing import Dict, Any, Optional, List
import pandas as pd


class DuckDBQueryEngine:
    """DuckDB查询引擎"""

    def __init__(
        self,
        cache_dir: str,
        memory_limit: str = "2GB",
        threads: int = 4,
        temp_directory: Optional[str] = None
    ):
        """
        初始化DuckDB查询引擎

        Args:
            cache_dir: 缓存目录
            memory_limit: 内存限制
            threads: 并行线程数
            temp_directory: 临时目录
        """
        self.cache_dir = cache_dir
        self.memory_limit = memory_limit
        self.threads = threads
        self.temp_directory = temp_directory or os.path.join(cache_dir, 'temp')

        # 确保临时目录存在
        os.makedirs(self.temp_directory, exist_ok=True)

        # 数据目录
        self.by_stock_dir = os.path.join(cache_dir, 'data', 'by_stock')
        self.by_date_dir = os.path.join(cache_dir, 'data', 'by_date')

        # DuckDB连接（延迟初始化）
        self._connection = None
        self._lock = threading.RLock()

        # 已注册的表
        self._registered_tables = set()

    @property
    def connection(self):
        """获取DuckDB连接（线程安全）"""
        if self._connection is None:
            try:
                import duckdb
                with self._lock:
                    if self._connection is None:
                        self._connection = duckdb.connect(':memory:')
                        self._configure_connection()
            except ImportError:
                raise ImportError("需要安装duckdb: pip install duckdb")

        return self._connection

    def execute_sql(self, sql: str, **params) -> pd.DataFrame:
        """
        执行SQL查询

        Args:
            sql: SQL查询语句
            **params: 查询参数

        Returns:
            pd.DataFrame: 查询结果
        """
        try:
            with self._lock:
                # 参数化查询
                if params:
                    sql = self._process_parameters(sql, params)

                # 执行查询
                result = self.connection.execute(sql).fetchdf()
                return result

        except Exception as e:
            raise Exception(f"SQL查询执行失败: {e}\nSQL: {sql}")

    def register_cache_tables(self) -> None:
        """注册缓存文件为DuckDB表"""
        try:
            with self._lock:
                # 注册K线数据表
                self._register_k_data_tables()

                # 注册横截面数据表
                self._register_cross_sectional_tables()

                # 注册财务数据表
                self._register_financial_tables()

        except Exception as e:
            print(f"注册缓存表失败: {e}")

    def create_virtual_table(self, table_name: str, file_pattern: str) -> None:
        """
        创建虚拟表

        Args:
            table_name: 表名
            file_pattern: 文件路径模式
        """
        try:
            with self._lock:
                if table_name not in self._registered_tables:
                    # 检查文件是否存在
                    import glob
                    files = glob.glob(file_pattern)
                    if files:
                        sql = f"""
                        CREATE OR REPLACE VIEW {table_name} AS
                        SELECT * FROM '{file_pattern}'
                        """
                        self.connection.execute(sql)
                        self._registered_tables.add(table_name)

        except Exception as e:
            print(f"创建虚拟表失败 {table_name}: {e}")

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        获取表信息

        Args:
            table_name: 表名

        Returns:
            Dict[str, Any]: 表信息
        """
        try:
            with self._lock:
                # 获取表结构
                schema_sql = f"DESCRIBE {table_name}"
                schema_result = self.connection.execute(schema_sql).fetchdf()

                # 获取行数
                count_sql = f"SELECT COUNT(*) as row_count FROM {table_name}"
                count_result = self.connection.execute(count_sql).fetchdf()

                return {
                    'table_name': table_name,
                    'columns': schema_result.to_dict('records'),
                    'row_count': count_result.iloc[0]['row_count'],
                    'exists': True
                }

        except Exception as e:
            return {
                'table_name': table_name,
                'columns': [],
                'row_count': 0,
                'exists': False,
                'error': str(e)
            }

    def list_tables(self) -> List[str]:
        """
        列出所有已注册的表

        Returns:
            List[str]: 表名列表
        """
        try:
            with self._lock:
                result = self.connection.execute("SHOW TABLES").fetchdf()
                return result['name'].tolist() if not result.empty else []

        except Exception:
            return list(self._registered_tables)

    def optimize_query(self, sql: str) -> str:
        """
        优化SQL查询

        Args:
            sql: 原始SQL

        Returns:
            str: 优化后的SQL
        """
        # 简单的查询优化逻辑
        optimized_sql = sql

        # 添加分区裁剪提示
        if 'WHERE' in sql.upper() and 'date' in sql.lower():
            # 如果有日期条件，可以添加分区裁剪
            pass

        # 添加列式存储优化
        if 'SELECT *' in sql.upper():
            # 建议用户指定需要的列
            pass

        return optimized_sql

    def explain_query(self, sql: str) -> str:
        """
        解释查询执行计划

        Args:
            sql: SQL查询语句

        Returns:
            str: 执行计划
        """
        try:
            with self._lock:
                explain_sql = f"EXPLAIN {sql}"
                result = self.connection.execute(explain_sql).fetchdf()
                return result.to_string()

        except Exception as e:
            return f"获取执行计划失败: {e}"

    def get_query_statistics(self) -> Dict[str, Any]:
        """
        获取查询统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        try:
            with self._lock:
                # 获取内存使用情况
                memory_sql = "SELECT * FROM duckdb_memory()"
                memory_result = self.connection.execute(memory_sql).fetchdf()

                # 获取已注册表的统计
                tables_info = {}
                for table_name in self.list_tables():
                    tables_info[table_name] = self.get_table_info(table_name)

                return {
                    'memory_usage': memory_result.to_dict('records'),
                    'registered_tables': list(self._registered_tables),
                    'tables_info': tables_info,
                    'cache_dir': self.cache_dir,
                    'config': {
                        'memory_limit': self.memory_limit,
                        'threads': self.threads,
                        'temp_directory': self.temp_directory
                    }
                }

        except Exception as e:
            return {
                'error': str(e),
                'registered_tables': list(self._registered_tables)
            }

    def cleanup(self) -> None:
        """清理资源"""
        try:
            with self._lock:
                if self._connection:
                    self._connection.close()
                    self._connection = None
                self._registered_tables.clear()

        except Exception as e:
            print(f"清理DuckDB资源失败: {e}")

    def _configure_connection(self) -> None:
        """配置DuckDB连接"""
        try:
            # 设置内存限制
            self.connection.execute(f"SET memory_limit='{self.memory_limit}'")

            # 设置线程数
            self.connection.execute(f"SET threads={self.threads}")

            # 设置临时目录
            self.connection.execute(f"SET temp_directory='{self.temp_directory}'")

            # 启用优化器
            self.connection.execute("SET enable_optimizer=true")

            # 设置其他性能参数
            self.connection.execute("SET enable_profiling=false")

        except Exception as e:
            print(f"配置DuckDB连接失败: {e}")

    def _register_k_data_tables(self) -> None:
        """注册K线数据表"""
        if not os.path.exists(self.by_stock_dir):
            return

        try:
            # 创建统一的K线数据视图
            k_data_pattern = os.path.join(self.by_stock_dir, '*/k_data_*/*.parquet')
            self.create_virtual_table('k_data', k_data_pattern)

            # 为不同的K线类型创建单独视图
            k_data_types = ['k_data_d_3', 'k_data_w_3', 'k_data_m_3']  # 日线、周线、月线
            for k_type in k_data_types:
                pattern = os.path.join(self.by_stock_dir, f'*/{k_type}/*.parquet')
                self.create_virtual_table(k_type, pattern)

        except Exception as e:
            print(f"注册K线数据表失败: {e}")

    def _register_cross_sectional_tables(self) -> None:
        """注册横截面数据表"""
        if not os.path.exists(self.by_date_dir):
            return

        try:
            # 注册市场数据表
            market_pattern = os.path.join(self.by_date_dir, 'market_data/*.parquet')
            self.create_virtual_table('market_data', market_pattern)

            # 注册技术指标表
            tech_pattern = os.path.join(self.by_date_dir, 'technical_indicators/*.parquet')
            self.create_virtual_table('technical_indicators', tech_pattern)

            # 注册资金流数据表
            fund_flow_pattern = os.path.join(self.by_date_dir, 'fund_flow/*.parquet')
            self.create_virtual_table('fund_flow', fund_flow_pattern)

        except Exception as e:
            print(f"注册横截面数据表失败: {e}")

    def _register_financial_tables(self) -> None:
        """注册财务数据表"""
        try:
            # 财务数据可能存储在两个位置
            # 1. 按股票分组的财务数据
            profit_pattern = os.path.join(self.by_stock_dir, '*/profit_data/*.parquet')
            self.create_virtual_table('profit_data', profit_pattern)

            balance_pattern = os.path.join(self.by_stock_dir, '*/balance_data/*.parquet')
            self.create_virtual_table('balance_data', balance_pattern)

            cash_flow_pattern = os.path.join(self.by_stock_dir, '*/cash_flow_data/*.parquet')
            self.create_virtual_table('cash_flow_data', cash_flow_pattern)

            # 2. 按日期分组的财务快照
            financial_snapshot_pattern = os.path.join(self.by_date_dir, 'financial_snapshot/*.parquet')
            self.create_virtual_table('financial_snapshot', financial_snapshot_pattern)

            # 创建统一的财务数据视图
            if 'profit_data' in self._registered_tables and 'balance_data' in self._registered_tables:
                financial_union_sql = """
                CREATE OR REPLACE VIEW financial_data AS
                SELECT 'profit' as data_type, * FROM profit_data
                UNION ALL
                SELECT 'balance' as data_type, * FROM balance_data
                UNION ALL
                SELECT 'cash_flow' as data_type, * FROM cash_flow_data
                """
                self.connection.execute(financial_union_sql)
                self._registered_tables.add('financial_data')

        except Exception as e:
            print(f"注册财务数据表失败: {e}")

    def _process_parameters(self, sql: str, params: Dict[str, Any]) -> str:
        """
        处理SQL参数化查询

        Args:
            sql: SQL模板
            params: 参数字典

        Returns:
            str: 处理后的SQL
        """
        processed_sql = sql

        for param_name, param_value in params.items():
            placeholder = f"${param_name}"
            if placeholder in processed_sql:
                # 根据参数类型处理
                if isinstance(param_value, str):
                    # 字符串参数加引号
                    processed_value = f"'{param_value}'"
                elif isinstance(param_value, (list, tuple)):
                    # 列表参数转换为IN子句
                    if all(isinstance(x, str) for x in param_value):
                        processed_value = "(" + ",".join(f"'{x}'" for x in param_value) + ")"
                    else:
                        processed_value = "(" + ",".join(str(x) for x in param_value) + ")"
                else:
                    # 数值参数直接替换
                    processed_value = str(param_value)

                processed_sql = processed_sql.replace(placeholder, processed_value)

        return processed_sql