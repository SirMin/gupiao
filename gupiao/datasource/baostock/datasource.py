"""
Baostock 数据源实现

基于 baostock SDK 实现的股票数据源，遵循统一的 StockDataSource 接口规范
"""

import logging
from typing import Optional, Union

try:
    import baostock as bs
except ImportError:
    bs = None
    logging.warning("baostock 未安装，请运行: pip install baostock")

from ..stock import StockDataSource, StockDataResult


class BaostockDataSource(StockDataSource):
    """Baostock 数据源实现类

    基于 baostock SDK 实现的股票数据源，提供完整的股票、财务、宏观经济等数据获取功能。
    需要先登录才能使用数据查询功能。
    """

    def __init__(self):
        """初始化 Baostock 数据源"""
        super().__init__()
        if bs is None:
            raise ImportError("baostock 未安装，请运行: pip install baostock")
        self._is_logged_in = False
        self._logger = logging.getLogger(__name__)
        login_result = self.login()
        if not login_result.success:
            self.logger.error(f"数据源登录失败: {login_result.error_msg}")
            raise RuntimeError(f"数据源登录失败: {login_result.error_msg}")


    def __del__(self):
        """析构时自动登出"""
        if hasattr(self, '_is_logged_in') and self._is_logged_in:
            try:
                bs.logout()
            except Exception:
                pass

    # ==================== 系统操作 ====================

    def login(self) -> StockDataResult:
        """登录 baostock 系统"""
        try:
            lg = bs.login()
            self._is_logged_in = (lg.error_code == '0')

            if self._is_logged_in:
                self._logger.info("Baostock 登录成功")
                return StockDataResult(
                    success=True,
                    error_code=lg.error_code,
                    error_msg=lg.error_msg or "登录成功"
                )
            else:
                self._logger.error(f"Baostock 登录失败: {lg.error_msg}")
                return StockDataResult(
                    success=False,
                    error_code=lg.error_code,
                    error_msg=lg.error_msg
                )
        except Exception as e:
            self._logger.error(f"Baostock 登录异常: {e}")
            return StockDataResult(
                success=False,
                error_code="BAOSTOCK_LOGIN_ERROR",
                error_msg=f"登录异常: {str(e)}"
            )

    def logout(self) -> StockDataResult:
        """登出 baostock 系统"""
        try:
            bs.logout()
            self._is_logged_in = False
            self._logger.info("Baostock 登出成功")
            return StockDataResult(
                success=True,
                error_code="0",
                error_msg="登出成功"
            )
        except Exception as e:
            self._logger.error(f"Baostock 登出异常: {e}")
            return StockDataResult(
                success=False,
                error_code="BAOSTOCK_LOGOUT_ERROR",
                error_msg=f"登出异常: {str(e)}"
            )

    @property
    def is_logged_in(self) -> bool:
        """检查是否已登录"""
        return self._is_logged_in

    def _check_login(self) -> bool:
        """检查登录状态"""
        if not self.is_logged_in:
            raise RuntimeError("请先调用 login() 方法登录 baostock 系统")
        return True

    def _convert_result(self, rs) -> StockDataResult:
        """将 baostock 结果转换为标准格式"""
        if rs.error_code != '0':
            return StockDataResult(
                success=False,
                error_code=rs.error_code,
                error_msg=rs.error_msg
            )

        # 提取数据
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        return StockDataResult(
            success=True,
            data=data_list,
            fields=rs.fields
        )

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
        """获取历史A股K线数据"""
        try:
            self._check_login()

            # 参数验证
            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if start_date and not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if end_date and not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            # 调用 baostock API
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )

            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询K线数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_dividend_data(
        self,
        code: str,
        year: str,
        yearType: str = "trade"
    ) -> StockDataResult:
        """查询除权除息信息"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            rs = bs.query_dividend_data(code=code, year=year, yearType=yearType)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询除权除息数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_adjust_factor(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询复权因子信息"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_adjust_factor(code=code, start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询复权因子数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    # ==================== 财务数据 ====================

    def query_profit_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频盈利能力数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_profit_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询盈利能力数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_operation_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频营运能力数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_operation_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询营运能力数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_growth_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频成长能力数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_growth_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询成长能力数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_balance_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频偿债能力数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_balance_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询偿债能力数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_cash_flow_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频现金流量数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_cash_flow_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询现金流量数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_dupont_data(
        self,
        code: str,
        year: Union[str, int],
        quarter: Optional[Union[str, int]] = None
    ) -> StockDataResult:
        """查询季频杜邦指数数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_year(year):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_YEAR",
                    error_msg="年份格式错误"
                )

            if not self._validate_quarter(quarter):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_QUARTER",
                    error_msg="季度参数错误"
                )

            rs = bs.query_dupont_data(code=code, year=str(year), quarter=str(quarter) if quarter else None)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询杜邦指数数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_performance_express_report(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询季频公司业绩快报数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_performance_express_report(code=code, start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询业绩快报数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_forecast_report(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询季频公司业绩预告数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_forecast_report(code=code, start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询业绩预告数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    # ==================== 证券信息 ====================

    def query_trade_dates(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询交易日信息"""
        try:
            self._check_login()

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询交易日数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_all_stock(
        self,
        day: Optional[str] = None
    ) -> StockDataResult:
        """查询所有股票代码"""
        try:
            self._check_login()

            if day and not self._validate_date(day):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_DATE",
                    error_msg="日期格式错误"
                )

            rs = bs.query_all_stock(day=day)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询股票列表异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_stock_basic(
        self,
        code: str,
        fields: Optional[str] = None
    ) -> StockDataResult:
        """查询证券基本资料"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            rs = bs.query_stock_basic(code=code, fields=fields)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询股票基本信息异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    # ==================== 宏观经济数据 ====================

    def query_deposit_rate_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询存款利率数据"""
        try:
            self._check_login()

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_deposit_rate_data(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询存款利率数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_loan_rate_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询贷款利率数据"""
        try:
            self._check_login()

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_loan_rate_data(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询贷款利率数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_required_reserve_ratio_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询存款准备金率数据"""
        try:
            self._check_login()

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_required_reserve_ratio_data(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询存款准备金率数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_money_supply_data_month(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询月度货币供应量数据"""
        try:
            self._check_login()

            if not self._validate_month_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误，应为YYYY-MM"
                )

            if not self._validate_month_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误，应为YYYY-MM"
                )

            rs = bs.query_money_supply_data_month(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询月度货币供应量数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_money_supply_data_year(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询年度货币供应量数据"""
        try:
            self._check_login()

            if not self._validate_year_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误，应为YYYY"
                )

            if not self._validate_year_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误，应为YYYY"
                )

            rs = bs.query_money_supply_data_year(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询年度货币供应量数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_shibor_data(
        self,
        start_date: str,
        end_date: str
    ) -> StockDataResult:
        """查询银行间同业拆放利率数据"""
        try:
            self._check_login()

            if not self._validate_date(start_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_START_DATE",
                    error_msg="开始日期格式错误"
                )

            if not self._validate_date(end_date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_END_DATE",
                    error_msg="结束日期格式错误"
                )

            rs = bs.query_shibor_data(start_date=start_date, end_date=end_date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询银行间同业拆放利率数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    # ==================== 板块数据 ====================

    def query_stock_industry(
        self,
        code: str,
        date: str
    ) -> StockDataResult:
        """查询行业分类数据"""
        try:
            self._check_login()

            if not self._validate_code(code):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_CODE",
                    error_msg="股票代码格式错误"
                )

            if not self._validate_date(date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_DATE",
                    error_msg="日期格式错误"
                )

            rs = bs.query_stock_industry(code=code, date=date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询行业分类数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_sz50_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询上证50成分股数据"""
        try:
            self._check_login()

            if not self._validate_date(date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_DATE",
                    error_msg="日期格式错误"
                )

            rs = bs.query_sz50_stocks(date=date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询上证50成分股数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_hs300_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询沪深300成分股数据"""
        try:
            self._check_login()

            if not self._validate_date(date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_DATE",
                    error_msg="日期格式错误"
                )

            rs = bs.query_hs300_stocks(date=date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询沪深300成分股数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )

    def query_zz500_stocks(
        self,
        date: str
    ) -> StockDataResult:
        """查询中证500成分股数据"""
        try:
            self._check_login()

            if not self._validate_date(date):
                return StockDataResult(
                    success=False,
                    error_code="INVALID_DATE",
                    error_msg="日期格式错误"
                )

            rs = bs.query_zz500_stocks(date=date)
            return self._convert_result(rs)

        except RuntimeError as e:
            return StockDataResult(
                success=False,
                error_code="NOT_LOGGED_IN",
                error_msg=str(e)
            )
        except Exception as e:
            self._logger.error(f"查询中证500成分股数据异常: {e}")
            return StockDataResult(
                success=False,
                error_code="QUERY_ERROR",
                error_msg=f"查询异常: {str(e)}"
            )