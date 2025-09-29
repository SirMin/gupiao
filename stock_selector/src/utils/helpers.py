"""
辅助函数模块 - 提供通用的工具函数
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
import re
from datetime import datetime, timedelta
import os


def validate_stock_code(code: str) -> bool:
    """
    验证股票代码格式

    Args:
        code: 股票代码

    Returns:
        是否为有效格式
    """
    if not isinstance(code, str):
        return False

    # 检查格式：sh.xxxxxx 或 sz.xxxxxx
    pattern = r'^(sh|sz)\.\d{6}$'
    return bool(re.match(pattern, code))


def normalize_stock_code(code: str) -> Optional[str]:
    """
    标准化股票代码格式

    Args:
        code: 输入的股票代码

    Returns:
        标准化后的股票代码，如果无效则返回None
    """
    if not isinstance(code, str):
        return None

    code = code.strip().lower()

    # 如果已经是标准格式
    if validate_stock_code(code):
        return code

    # 尝试补全格式
    if len(code) == 6 and code.isdigit():
        # 根据代码判断交易所
        if code.startswith('6'):
            return f'sh.{code}'
        elif code.startswith(('0', '3')):
            return f'sz.{code}'

    return None


def get_trading_dates(start_date: str, end_date: str, exclude_weekends: bool = True) -> List[str]:
    """
    获取交易日期列表

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        exclude_weekends: 是否排除周末

    Returns:
        交易日期列表
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        dates = []
        current = start

        while current <= end:
            if not exclude_weekends or current.weekday() < 5:  # 0-4为周一到周五
                dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        return dates

    except ValueError:
        return []


def get_previous_trading_date(date: str, days: int = 1) -> str:
    """
    获取指定日期前N个交易日

    Args:
        date: 基准日期 (YYYY-MM-DD)
        days: 前推天数

    Returns:
        前N个交易日日期
    """
    try:
        base_date = datetime.strptime(date, '%Y-%m-%d')
        current = base_date

        trading_days_found = 0
        while trading_days_found < days:
            current -= timedelta(days=1)
            # 排除周末
            if current.weekday() < 5:
                trading_days_found += 1

        return current.strftime('%Y-%m-%d')

    except ValueError:
        return date


def format_number(value: Union[int, float], precision: int = 2, unit: str = '') -> str:
    """
    格式化数字显示

    Args:
        value: 数值
        precision: 精度
        unit: 单位

    Returns:
        格式化后的字符串
    """
    if pd.isna(value):
        return 'N/A'

    if abs(value) >= 1e8:
        return f'{value / 1e8:.{precision}f}亿{unit}'
    elif abs(value) >= 1e4:
        return f'{value / 1e4:.{precision}f}万{unit}'
    else:
        return f'{value:.{precision}f}{unit}'


def format_percentage(value: Union[int, float], precision: int = 2) -> str:
    """
    格式化百分比显示

    Args:
        value: 数值 (0.1 表示 10%)
        precision: 精度

    Returns:
        格式化后的百分比字符串
    """
    if pd.isna(value):
        return 'N/A'

    return f'{value * 100:.{precision}f}%'


def calculate_market_value_tier(market_value: float) -> str:
    """
    根据市值计算市值分层

    Args:
        market_value: 市值（亿元）

    Returns:
        市值分层字符串
    """
    if pd.isna(market_value):
        return '未知'

    if market_value < 100:
        return '小盘股'
    elif market_value <= 500:
        return '中盘股'
    else:
        return '大盘股'


def safe_divide(numerator: Union[int, float], denominator: Union[int, float], default: float = 0.0) -> float:
    """
    安全除法，避免除零错误

    Args:
        numerator: 分子
        denominator: 分母
        default: 默认值

    Returns:
        计算结果或默认值
    """
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return default

    return numerator / denominator


def winsorize_series(series: pd.Series, lower_percentile: float = 0.01, upper_percentile: float = 0.99) -> pd.Series:
    """
    对Series进行Winsorize处理，限制极值

    Args:
        series: 输入Series
        lower_percentile: 下分位数
        upper_percentile: 上分位数

    Returns:
        处理后的Series
    """
    if series.empty:
        return series

    lower_bound = series.quantile(lower_percentile)
    upper_bound = series.quantile(upper_percentile)

    return series.clip(lower=lower_bound, upper=upper_bound)


def calculate_correlation_matrix(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    计算相关性矩阵

    Args:
        data: 输入数据
        columns: 要计算相关性的列

    Returns:
        相关性矩阵
    """
    if data.empty:
        return pd.DataFrame()

    valid_columns = [col for col in columns if col in data.columns]

    if not valid_columns:
        return pd.DataFrame()

    return data[valid_columns].corr()


def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
    """
    使用IQR方法检测异常值

    Args:
        series: 输入Series
        multiplier: IQR倍数

    Returns:
        布尔Series，True表示异常值
    """
    if series.empty:
        return pd.Series(dtype=bool)

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    return (series < lower_bound) | (series > upper_bound)


def create_summary_statistics(data: pd.DataFrame, columns: List[str]) -> Dict[str, Dict[str, float]]:
    """
    创建汇总统计信息

    Args:
        data: 输入数据
        columns: 要分析的列

    Returns:
        汇总统计字典
    """
    summary = {}

    for column in columns:
        if column not in data.columns:
            continue

        series = data[column].dropna()
        if series.empty:
            continue

        summary[column] = {
            'count': len(series),
            'mean': series.mean(),
            'std': series.std(),
            'min': series.min(),
            'max': series.max(),
            'median': series.median(),
            'q25': series.quantile(0.25),
            'q75': series.quantile(0.75),
            'skewness': series.skew(),
            'kurtosis': series.kurtosis()
        }

    return summary


def ensure_directory_exists(file_path: str):
    """
    确保文件所在目录存在

    Args:
        file_path: 文件路径
    """
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def clean_dataframe(df: pd.DataFrame,
                   drop_duplicates: bool = True,
                   drop_na_columns: List[str] = None,
                   fill_na_values: Dict[str, Any] = None) -> pd.DataFrame:
    """
    清理DataFrame

    Args:
        df: 输入DataFrame
        drop_duplicates: 是否删除重复行
        drop_na_columns: 需要删除空值的列
        fill_na_values: 填充空值的字典

    Returns:
        清理后的DataFrame
    """
    if df.empty:
        return df

    result = df.copy()

    # 删除重复行
    if drop_duplicates:
        result = result.drop_duplicates()

    # 删除指定列的空值行
    if drop_na_columns:
        valid_columns = [col for col in drop_na_columns if col in result.columns]
        if valid_columns:
            result = result.dropna(subset=valid_columns)

    # 填充空值
    if fill_na_values:
        for column, value in fill_na_values.items():
            if column in result.columns:
                result[column] = result[column].fillna(value)

    return result


def validate_date_format(date_str: str) -> bool:
    """
    验证日期格式

    Args:
        date_str: 日期字符串

    Returns:
        是否为有效日期格式
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def get_date_range_info(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    获取日期范围信息

    Args:
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        日期范围信息
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        delta = end - start
        trading_days = len(get_trading_dates(start_date, end_date))

        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': delta.days + 1,
            'trading_days': trading_days,
            'weeks': (delta.days + 1) // 7,
            'months': (end.year - start.year) * 12 + (end.month - start.month)
        }

    except ValueError:
        return {}


def create_bins(series: pd.Series, bins: int = 10, labels: List[str] = None) -> pd.Series:
    """
    创建分箱

    Args:
        series: 输入Series
        bins: 分箱数量
        labels: 分箱标签

    Returns:
        分箱后的Series
    """
    if series.empty:
        return series

    try:
        return pd.cut(series, bins=bins, labels=labels, include_lowest=True)
    except Exception:
        return pd.Series(index=series.index, dtype='category')


def calculate_rolling_statistics(series: pd.Series, window: int) -> Dict[str, pd.Series]:
    """
    计算滚动统计量

    Args:
        series: 输入Series
        window: 滚动窗口大小

    Returns:
        滚动统计量字典
    """
    if series.empty:
        return {}

    return {
        'mean': series.rolling(window).mean(),
        'std': series.rolling(window).std(),
        'min': series.rolling(window).min(),
        'max': series.rolling(window).max(),
        'median': series.rolling(window).median()
    }


def merge_dataframes_safe(left: pd.DataFrame, right: pd.DataFrame,
                         on: Union[str, List[str]], how: str = 'inner') -> pd.DataFrame:
    """
    安全地合并DataFrame

    Args:
        left: 左DataFrame
        right: 右DataFrame
        on: 合并键
        how: 合并方式

    Returns:
        合并后的DataFrame
    """
    if left.empty or right.empty:
        return left if not left.empty else right

    try:
        return pd.merge(left, right, on=on, how=how)
    except Exception:
        return left