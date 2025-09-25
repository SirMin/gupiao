"""
Baostock 数据源使用示例

演示如何使用 BaostockDataSource 获取各种股票数据
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from gupiao.datasource.baostock import BaostockDataSource


def main():
    """主函数：演示各种数据查询功能"""
    print("=== Baostock 数据源使用示例 ===\n")

    # 创建数据源实例
    bs_data = BaostockDataSource()

    try:
        # 1. 登录
        print("1. 登录系统")
        login_result = bs_data.login()
        if login_result.success:
            print(f"✓ 登录成功: {login_result.error_msg}")
        else:
            print(f"✗ 登录失败: {login_result.error_msg}")
            return
        print()

        # 2. 查询K线数据
        print("2. 查询K线数据")
        kline_result = bs_data.query_history_k_data_plus(
            code="sh.600000",
            fields="date,code,open,high,low,close,volume,amount",
            start_date="2024-01-01",
            end_date="2024-01-05",
            frequency="d",
            adjustflag="3"
        )

        if kline_result.success:
            df = kline_result.to_dataframe()
            print(f"✓ K线数据查询成功，共 {len(df)} 条记录")
            if not df.empty:
                print("前3条数据预览:")
                print(df.head(3))
        else:
            print(f"✗ K线数据查询失败: {kline_result.error_msg}")
        print()

        # 3. 查询交易日
        print("3. 查询交易日")
        trade_dates_result = bs_data.query_trade_dates("2024-01-01", "2024-01-05")
        if trade_dates_result.success:
            df = trade_dates_result.to_dataframe()
            print(f"✓ 交易日查询成功，共 {len(df)} 条记录")
            if not df.empty:
                print("交易日数据:")
                print(df)
        else:
            print(f"✗ 交易日查询失败: {trade_dates_result.error_msg}")
        print()

        # 4. 查询股票列表（限制数量）
        print("4. 查询股票列表")
        stocks_result = bs_data.query_all_stock("2024-01-02")
        if stocks_result.success:
            df = stocks_result.to_dataframe()
            print(f"✓ 股票列表查询成功，共 {len(df)} 只股票")
            if not df.empty:
                print("前5只股票:")
                print(df.head(5))
        else:
            print(f"✗ 股票列表查询失败: {stocks_result.error_msg}")
        print()

        # 5. 查询股票基本信息
        print("5. 查询股票基本信息")
        basic_result = bs_data.query_stock_basic("sh.600000")
        if basic_result.success:
            df = basic_result.to_dataframe()
            print(f"✓ 基本信息查询成功")
            if not df.empty:
                print("股票基本信息:")
                print(df)
        else:
            print(f"✗ 基本信息查询失败: {basic_result.error_msg}")
        print()

        # 6. 查询财务数据
        print("6. 查询财务数据（盈利能力）")
        profit_result = bs_data.query_profit_data("sh.600000", 2023, 4)
        if profit_result.success:
            df = profit_result.to_dataframe()
            print(f"✓ 盈利能力数据查询成功")
            if not df.empty:
                print("盈利能力数据:")
                print(df)
            else:
                print("未找到相关财务数据")
        else:
            print(f"✗ 盈利能力数据查询失败: {profit_result.error_msg}")
        print()

        # 7. 查询除权除息信息
        print("7. 查询除权除息信息")
        dividend_result = bs_data.query_dividend_data("sh.600000", "2023")
        if dividend_result.success:
            df = dividend_result.to_dataframe()
            print(f"✓ 除权除息数据查询成功")
            if not df.empty:
                print("除权除息数据:")
                print(df)
            else:
                print("未找到相关除权除息数据")
        else:
            print(f"✗ 除权除息数据查询失败: {dividend_result.error_msg}")
        print()

        # 8. 查询沪深300成分股
        print("8. 查询沪深300成分股")
        hs300_result = bs_data.query_hs300_stocks("2024-01-02")
        if hs300_result.success:
            df = hs300_result.to_dataframe()
            print(f"✓ 沪深300成分股查询成功，共 {len(df)} 只股票")
            if not df.empty:
                print("前5只成分股:")
                print(df.head(5))
        else:
            print(f"✗ 沪深300成分股查询失败: {hs300_result.error_msg}")
        print()

        # 9. 查询存款利率数据
        print("9. 查询存款利率数据")
        deposit_rate_result = bs_data.query_deposit_rate_data("2015-01-01", "2015-12-31")
        if deposit_rate_result.success:
            df = deposit_rate_result.to_dataframe()
            print(f"✓ 存款利率数据查询成功")
            if not df.empty:
                print("存款利率数据:")
                print(df.head(3))
            else:
                print("未找到相关存款利率数据")
        else:
            print(f"✗ 存款利率数据查询失败: {deposit_rate_result.error_msg}")
        print()

    except Exception as e:
        print(f"程序执行异常: {e}")

    finally:
        # 登出
        print("10. 登出系统")
        logout_result = bs_data.logout()
        if logout_result.success:
            print(f"✓ 登出成功: {logout_result.error_msg}")
        else:
            print(f"✗ 登出失败: {logout_result.error_msg}")


def test_error_handling():
    """测试错误处理"""
    print("\n=== 错误处理测试 ===\n")

    bs_data = BaostockDataSource()

    # 1. 未登录状态下查询数据
    print("1. 测试未登录状态")
    result = bs_data.query_history_k_data_plus("sh.600000", "date,close", "2024-01-01", "2024-01-02")
    print(f"未登录查询结果: success={result.success}, error_code={result.error_code}")
    print()

    # 登录
    bs_data.login()

    # 2. 测试无效股票代码
    print("2. 测试无效股票代码")
    result = bs_data.query_history_k_data_plus("invalid_code", "date,close", "2024-01-01", "2024-01-02")
    print(f"无效代码查询结果: success={result.success}, error_code={result.error_code}")
    print()

    # 3. 测试无效日期格式
    print("3. 测试无效日期格式")
    result = bs_data.query_history_k_data_plus("sh.600000", "date,close", "invalid_date", "2024-01-02")
    print(f"无效日期查询结果: success={result.success}, error_code={result.error_code}")
    print()

    # 登出
    bs_data.logout()


if __name__ == "__main__":
    # 运行主示例
    main()

    # 运行错误处理测试
    test_error_handling()

    print("\n=== 示例程序结束 ===")