# Makefile for gupiao project

.PHONY: test test-cache test-verbose clean install dev-install

# 运行所有测试
test:
	python -m pytest tests/

# 只运行缓存测试
test-cache:
	python -m pytest tests/datasource/cache/

# 详细模式运行测试
test-verbose:
	python -m pytest tests/ -v

# 运行特定测试文件
test-storage:
	python -m pytest tests/datasource/cache/test_storage.py -v

test-range:
	python -m pytest tests/datasource/cache/test_range_calculator.py -v

test-metadata:
	python -m pytest tests/datasource/cache/test_metadata_manager.py -v

test-datasource-manager:
	python -m pytest tests/datasource/cache/test_datasource_manager.py -v

# 生成测试覆盖率报告
test-coverage:
	python -m pytest tests/ --cov=gupiao --cov-report=html --cov-report=term

# 运行测试并查看失败详情
test-failed:
	python -m pytest tests/ --tb=long --showlocals

# 使用unittest运行测试
test-unittest:
	python tests/test_runner.py

test-unittest-cache:
	python tests/test_runner.py --cache

# 清理测试产生的文件
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/

# 安装项目依赖
install:
	pip install -r requirements.txt

# 安装开发依赖
dev-install:
	pip install -r requirements.txt
	pip install pytest pytest-cov pytest-mock

# 检查代码风格
lint:
	flake8 gupiao tests

# 格式化代码
format:
	black gupiao tests

# 类型检查
type-check:
	mypy gupiao

# 运行所有检查
check: lint type-check test