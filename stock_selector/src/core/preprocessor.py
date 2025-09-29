"""
数据预处理模块 - 基于现有数据源接口进行数据预处理
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到路径中以便导入数据源模块
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../gupiao'))
from datasource.stock.base import StockDataSource


class StockDataPreprocessor:
    """股票数据预处理器

    基于现有的数据源接口，对原始股票数据进行清洗、计算技术指标等预处理操作
    """

    def __init__(self, data_source: StockDataSource):
        """
        初始化预处理器

        Args:
            data_source: 股票数据源实例
        """
        self.data_source = data_source
        self.logger = logging.getLogger(__name__)

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        Args:
            df: 包含OHLCV数据的DataFrame

        Returns:
            添加了技术指标的DataFrame
        """
        if df.empty:
            return df

        result_df = df.copy()

        try:
            # 确保数据按日期排序
            if 'date' in result_df.columns:
                result_df = result_df.sort_values('date')
                result_df.reset_index(drop=True, inplace=True)

            # 计算移动平均线
            result_df = self._calculate_moving_averages(result_df)

            # 计算连续上涨天数
            result_df = self._calculate_consecutive_up_days(result_df)

            # 计算累计收益率
            result_df = self._calculate_cumulative_returns(result_df)

            # 计算换手率相关指标
            result_df = self._calculate_turnover_metrics(result_df)

            # 计算波动率指标
            result_df = self._calculate_volatility_metrics(result_df)

            # 计算价格位置指标
            result_df = self._calculate_price_position(result_df)

            self.logger.info(f"技术指标计算完成，数据行数: {len(result_df)}")

        except Exception as e:
            self.logger.error(f"计算技术指标时出错: {str(e)}")

        return result_df

    def _calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算移动平均线"""
        if 'close' not in df.columns:
            return df

        # 计算5日、10日、20日移动平均线
        df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['ma10'] = df['close'].rolling(window=10, min_periods=1).mean()
        df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()

        # 计算均线状态
        df['ma5_gt_ma10'] = df['ma5'] > df['ma10']
        df['ma10_gt_ma20'] = df['ma10'] > df['ma20']
        df['ma_bullish'] = df['ma5_gt_ma10'] & df['ma10_gt_ma20']  # 多头排列
        df['close_gt_ma5'] = df['close'] > df['ma5']

        return df

    def _calculate_consecutive_up_days(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算连续上涨天数"""
        if 'close' not in df.columns or 'preclose' not in df.columns:
            # 如果没有preclose，用前一日收盘价
            if 'close' in df.columns:
                df['prev_close'] = df['close'].shift(1)
                df['daily_return'] = (df['close'] - df['prev_close']) / df['prev_close']
            else:
                return df
        else:
            df['daily_return'] = (df['close'] - df['preclose']) / df['preclose']

        # 判断当日是否上涨
        df['is_up'] = df['daily_return'] > 0

        # 计算连续上涨天数
        df['consecutive_up_days'] = 0
        consecutive_count = 0

        for i in range(len(df)):
            if df.iloc[i]['is_up']:
                consecutive_count += 1
            else:
                consecutive_count = 0
            df.iloc[i, df.columns.get_loc('consecutive_up_days')] = consecutive_count

        return df

    def _calculate_cumulative_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算累计收益率"""
        if 'daily_return' not in df.columns:
            return df

        # 计算3日累计收益率
        df['return_3d'] = df['daily_return'].rolling(window=3, min_periods=1).apply(
            lambda x: (1 + x).prod() - 1
        )

        # 计算5日累计收益率
        df['return_5d'] = df['daily_return'].rolling(window=5, min_periods=1).apply(
            lambda x: (1 + x).prod() - 1
        )

        return df

    def _calculate_turnover_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算换手率相关指标"""
        if 'turn' not in df.columns:
            return df

        # 转换换手率为小数形式（如果是百分比）
        if df['turn'].max() > 1:
            df['turnover'] = df['turn'] / 100
        else:
            df['turnover'] = df['turn']

        # 计算60日平均换手率
        df['avg_turnover_60d'] = df['turnover'].rolling(window=60, min_periods=20).mean()

        # 计算3日累计换手率
        df['cumulative_turnover_3d'] = df['turnover'].rolling(window=3, min_periods=1).sum()

        # 计算相对换手率
        df['relative_turnover'] = df['turnover'] / df['avg_turnover_60d']

        # 活跃度指标需要市值数据
        if 'amount' in df.columns:
            # 估算市值（成交额/换手率）
            df['estimated_market_value'] = np.where(
                df['turnover'] > 0,
                df['amount'] / df['turnover'],
                np.nan
            )
            # 计算活跃度
            df['activity_score'] = df['turnover'] * np.log(df['estimated_market_value'].fillna(1))

        return df

    def _calculate_volatility_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算波动率指标"""
        if 'high' not in df.columns or 'low' not in df.columns or 'close' not in df.columns:
            return df

        # 计算日振幅
        df['daily_amplitude'] = (df['high'] - df['low']) / df['close'].shift(1)

        # 计算3日收盘价标准差
        df['close_std_3d'] = df['close'].rolling(window=3, min_periods=2).std()

        # 检测极端波动（单日涨跌幅超过5%）
        if 'daily_return' in df.columns:
            df['extreme_volatility'] = np.abs(df['daily_return']) > 0.05

        return df

    def _calculate_price_position(self, df: pd.DataFrame, window: int = 252) -> pd.DataFrame:
        """计算价格位置指标"""
        if 'close' not in df.columns:
            return df

        # 计算指定窗口期内的最高价和最低价
        df['highest_price'] = df['close'].rolling(window=window, min_periods=20).max()
        df['lowest_price'] = df['close'].rolling(window=window, min_periods=20).min()

        # 计算价格位置（0-1之间，0表示最低位，1表示最高位）
        df['price_position'] = (df['close'] - df['lowest_price']) / (
            df['highest_price'] - df['lowest_price']
        )

        return df

    def get_stock_pool_data(self, date: str, stock_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取股票池数据

        Args:
            date: 交易日期
            stock_codes: 股票代码列表，如果为None则获取所有A股

        Returns:
            包含所有股票当日数据的DataFrame
        """
        all_data = []

        try:
            # 如果没有指定股票列表，获取所有A股
            if stock_codes is None:
                self.logger.info("获取A股股票列表...")
                result = self.data_source.query_all_stock(date)
                if not result.success:
                    self.logger.error(f"获取股票列表失败: {result.error_msg}")
                    return pd.DataFrame()

                stock_df = result.to_dataframe()
                # 过滤A股
                stock_codes = stock_df[
                    (stock_df['code'].str.startswith('sh.6')) |
                    (stock_df['code'].str.startswith('sz.0')) |
                    (stock_df['code'].str.startswith('sz.3'))
                ]['code'].tolist()

            self.logger.info(f"开始获取 {len(stock_codes)} 只股票的数据...")

            # 获取每只股票的历史数据用于计算技术指标
            for i, code in enumerate(stock_codes):
                if i % 100 == 0:
                    self.logger.info(f"处理进度: {i}/{len(stock_codes)}")

                # 获取足够的历史数据用于计算技术指标
                start_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=300)).strftime('%Y-%m-%d')

                result = self.data_source.query_history_k_data_plus(
                    code=code,
                    fields="date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,isST",
                    start_date=start_date,
                    end_date=date
                )

                if result.success:
                    stock_data = result.to_dataframe()
                    if not stock_data.empty:
                        # 数据类型转换
                        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
                        for col in numeric_columns:
                            if col in stock_data.columns:
                                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')

                        stock_data['date'] = pd.to_datetime(stock_data['date'])

                        # 计算技术指标
                        stock_data = self.calculate_technical_indicators(stock_data)

                        # 只保留目标日期的数据
                        target_data = stock_data[stock_data['date'] == date]
                        if not target_data.empty:
                            all_data.append(target_data)

        except Exception as e:
            self.logger.error(f"获取股票池数据时出错: {str(e)}")

        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"成功获取 {len(result_df)} 只股票的数据")
            return result_df
        else:
            self.logger.warning("未获取到任何股票数据")
            return pd.DataFrame()

    def filter_valid_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤有效股票数据

        Args:
            df: 股票数据DataFrame

        Returns:
            过滤后的DataFrame
        """
        if df.empty:
            return df

        initial_count = len(df)

        # 过滤ST股票
        if 'isST' in df.columns:
            df = df[df['isST'] != '1']

        # 过滤停牌股票
        if 'tradestatus' in df.columns:
            df = df[df['tradestatus'] == '1']

        # 过滤空值过多的数据
        required_columns = ['close', 'volume', 'amount']
        for col in required_columns:
            if col in df.columns:
                df = df.dropna(subset=[col])

        # 过滤异常数据
        if 'close' in df.columns:
            df = df[df['close'] > 0]

        if 'volume' in df.columns:
            df = df[df['volume'] > 0]

        filtered_count = len(df)
        self.logger.info(f"数据过滤完成: {initial_count} -> {filtered_count}")

        return df

    def get_market_data(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取大盘指数数据

        Args:
            index_code: 指数代码 (如: 'sh.000300' 沪深300)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            指数数据DataFrame
        """
        try:
            result = self.data_source.query_history_k_data_plus(
                code=index_code,
                fields="date,code,open,high,low,close,preclose,volume,amount,pctChg",
                start_date=start_date,
                end_date=end_date
            )

            if result.success:
                df = result.to_dataframe()
                if not df.empty:
                    # 数据类型转换
                    numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'pctChg']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')

                    df['date'] = pd.to_datetime(df['date'])

                    # 计算技术指标
                    df = self.calculate_technical_indicators(df)

                return df
            else:
                self.logger.error(f"获取指数数据失败: {result.error_msg}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取指数数据时出错: {str(e)}")
            return pd.DataFrame()

    def estimate_market_value(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        估算市值（基于成交额和换手率）

        Args:
            df: 股票数据DataFrame

        Returns:
            添加了市值估算的DataFrame
        """
        if 'amount' in df.columns and 'turnover' in df.columns:
            df['estimated_market_value'] = np.where(
                df['turnover'] > 0,
                df['amount'] / df['turnover'],
                np.nan
            )

            # 转换为亿元单位
            df['market_value_billion'] = df['estimated_market_value'] / 1e8

        return df