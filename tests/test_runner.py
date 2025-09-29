"""测试运行器"""

import unittest
import sys
import os

# 项目现在使用Poetry标准结构，无需手动添加路径


def run_all_tests():
    """运行所有测试"""
    # 发现并运行所有测试
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test_*.py')

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_cache_tests():
    """只运行缓存相关测试"""
    loader = unittest.TestLoader()
    cache_test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datasource', 'cache')
    suite = loader.discover(cache_test_dir, pattern='test_*.py')

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_specific_test(test_module):
    """运行特定测试模块"""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(test_module)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='运行测试')
    parser.add_argument('--cache', action='store_true', help='只运行缓存测试')
    parser.add_argument('--module', type=str, help='运行特定测试模块')

    args = parser.parse_args()

    if args.cache:
        success = run_cache_tests()
    elif args.module:
        success = run_specific_test(args.module)
    else:
        success = run_all_tests()

    sys.exit(0 if success else 1)