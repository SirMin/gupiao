#!/usr/bin/env python3
"""
股票选择策略系统主程序入口

使用方法:
python main.py --date 2024-01-15
python main.py --date 2024-01-15 --config config/simple_config.yaml
python main.py --batch --start-date 2024-01-01 --end-date 2024-01-07
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from typing import List


try:
    # 当作为包模块运行时使用相对导入
    from .core.runner import StockSelectorRunner
    from .utils.helpers import get_trading_dates, validate_date_format
except ImportError:
    # 当直接运行时使用绝对导入
    import sys
    import os
    # 添加项目根目录到 Python 路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(current_dir, '../../..')
    sys.path.insert(0, project_root)

    from gupiao.selector.core.runner import StockSelectorRunner
    from gupiao.selector.utils.helpers import get_trading_dates, validate_date_format


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='股票选择策略系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 单日选股
  python main.py --date 2024-01-15

  # 使用指定配置
  python main.py --date 2024-01-15 --config config/simple_config.yaml

  # 批量选股
  python main.py --batch --start-date 2024-01-01 --end-date 2024-01-07

  # 指定股票池
  python main.py --date 2024-01-15 --stocks sh.600000,sz.000001
        """
    )

    # 运行模式
    parser.add_argument(
        '--batch',
        action='store_true',
        help='批量模式'
    )

    # 日期参数
    parser.add_argument(
        '--date',
        type=str,
        help='目标日期 (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='批量模式的开始日期 (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='批量模式的结束日期 (YYYY-MM-DD)'
    )

    # 配置文件
    parser.add_argument(
        '--config',
        type=str,
        default='selector/config/default_config.yaml',
        help='配置文件路径'
    )

    # 股票代码
    parser.add_argument(
        '--stocks',
        type=str,
        help='指定股票代码，用逗号分隔 (例: sh.600000,sz.000001)'
    )

    # 输出选项
    parser.add_argument(
        '--output',
        type=str,
        help='输出目录路径'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='详细输出模式'
    )

    return parser.parse_args()


def validate_arguments(args):
    """验证命令行参数"""
    errors = []

    if args.batch:
        # 批量模式验证
        if not args.start_date or not args.end_date:
            errors.append("批量模式需要指定 --start-date 和 --end-date")

        if args.start_date and not validate_date_format(args.start_date):
            errors.append(f"开始日期格式无效: {args.start_date}")

        if args.end_date and not validate_date_format(args.end_date):
            errors.append(f"结束日期格式无效: {args.end_date}")

        if (args.start_date and args.end_date and
            validate_date_format(args.start_date) and validate_date_format(args.end_date)):
            start = datetime.strptime(args.start_date, '%Y-%m-%d')
            end = datetime.strptime(args.end_date, '%Y-%m-%d')
            if start > end:
                errors.append("开始日期不能晚于结束日期")
    else:
        # 单日模式验证
        if not args.date:
            # 默认使用昨天
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            args.date = yesterday
            print(f"未指定日期，使用默认日期: {args.date}")

        if not validate_date_format(args.date):
            errors.append(f"日期格式无效: {args.date}")

    # 配置文件检查
    if not os.path.exists(args.config):
        errors.append(f"配置文件不存在: {args.config}")

    return errors


def parse_stock_codes(stocks_str: str) -> List[str]:
    """解析股票代码字符串"""
    if not stocks_str:
        return None

    codes = []
    for code in stocks_str.split(','):
        code = code.strip()
        if code:
            codes.append(code)

    return codes if codes else None


def print_results_summary(result):
    """打印结果摘要"""
    if not result.get('success', False):
        print(f"❌ 执行失败: {result.get('error', '未知错误')}")
        return

    data_summary = result.get('data_summary', {})
    strategy_info = result.get('strategy_info', {})

    print("✅ 执行成功!")
    print(f"📊 数据概况:")
    print(f"   总股票数: {data_summary.get('total_stocks', 0)}")
    print(f"   有效股票数: {data_summary.get('valid_stocks', 0)}")
    print(f"   候选股票数: {data_summary.get('candidate_stocks', 0)}")
    print(f"   最终候选数: {data_summary.get('final_candidates', 0)}")

    print(f"🎯 策略信息:")
    print(f"   使用策略: {strategy_info.get('strategy_used', '未知')}")
    print(f"   策略级别: {strategy_info.get('strategy_level', '未知')}")

    print(f"⏱️  执行时间: {result.get('execution_time', 0):.2f}秒")

    report_files = result.get('report_files', [])
    if report_files:
        print(f"📄 生成报告:")
        for file_path in report_files:
            print(f"   {file_path}")


def print_batch_summary(batch_result):
    """打印批量处理摘要"""
    print(f"📊 批量处理完成:")
    print(f"   总日期数: {batch_result.get('total_dates', 0)}")
    print(f"   成功日期数: {batch_result.get('successful_dates', 0)}")
    print(f"   失败日期数: {batch_result.get('failed_dates', 0)}")

    errors = batch_result.get('errors', [])
    if errors:
        print(f"❌ 失败详情:")
        for error in errors[:5]:  # 只显示前5个错误
            print(f"   {error.get('date', '')}: {error.get('error', '')}")

        if len(errors) > 5:
            print(f"   ... 还有 {len(errors) - 5} 个错误")


def main():
    """主函数"""
    # 解析参数
    args = parse_arguments()

    # 验证参数
    errors = validate_arguments(args)
    if errors:
        print("❌ 参数错误:")
        for error in errors:
            print(f"   {error}")
        sys.exit(1)

    # 设置详细输出
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # 初始化运行器
        print(f"🚀 初始化股票选择器...")
        print(f"📝 配置文件: {args.config}")

        with StockSelectorRunner(args.config) as runner:

            # 解析股票代码
            stock_codes = parse_stock_codes(args.stocks)
            if stock_codes:
                print(f"🎯 指定股票池: {len(stock_codes)} 只股票")

            if args.batch:
                # 批量模式
                print(f"📅 批量模式: {args.start_date} 到 {args.end_date}")

                # 获取交易日期列表
                date_list = get_trading_dates(args.start_date, args.end_date)
                print(f"📊 交易日期数: {len(date_list)}")

                # 执行批量选股
                batch_result = runner.run_batch_selection(date_list, stock_codes)

                # 打印结果
                print_batch_summary(batch_result)

                # 生成汇总报告
                if batch_result.get('success', False):
                    try:
                        from .reporters.csv_reporter import CSVReporter
                    except ImportError:
                        from gupiao.selector.reporters.csv_reporter import CSVReporter
                    output_config = runner.config.get('output', {})
                    if args.output:
                        output_config['output_path'] = args.output

                    reporter = CSVReporter(output_config)
                    summary_file = reporter.generate_summary_report(batch_result.get('results', []))

                    if summary_file:
                        print(f"📄 批量汇总报告: {summary_file}")

            else:
                # 单日模式
                print(f"📅 目标日期: {args.date}")

                # 执行选股
                result = runner.run_stock_selection(args.date, stock_codes)

                # 打印结果
                print_results_summary(result)

                # 显示候选股票
                if result.get('success', False):
                    candidates = result.get('candidates', [])
                    if candidates and args.verbose:
                        print(f"\n🏆 Top {min(len(candidates), 10)} 候选股票:")
                        print("-" * 80)
                        print(f"{'排名':<4} {'代码':<10} {'名称':<12} {'综合评分':<8} {'收盘价':<8} {'3日涨幅':<8}")
                        print("-" * 80)

                        for i, candidate in enumerate(candidates[:10], 1):
                            code = candidate.get('code', '')
                            name = candidate.get('name', '')[:10]  # 限制名称长度
                            score = candidate.get('total_score', 0)
                            close = candidate.get('close', 0)
                            return_3d = candidate.get('return_3d', 0) * 100  # 转换为百分比

                            print(f"{i:<4} {code:<10} {name:<12} {score:<8.4f} {close:<8.2f} {return_3d:<8.2f}%")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        sys.exit(130)

    except Exception as e:
        print(f"❌ 系统错误: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()