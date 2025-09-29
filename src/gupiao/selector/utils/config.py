"""
配置管理模块 - 管理系统配置参数
"""
import yaml
import json
import os
from typing import Dict, Any, Optional
import logging
from datetime import datetime


class ConfigManager:
    """配置管理器

    负责加载、验证、保存和管理系统配置参数
    支持YAML和JSON格式配置文件
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径，如果为None则使用默认配置
        """
        self.logger = logging.getLogger(__name__)
        self.config_path = config_path
        self.config = {}

        # 加载配置
        if config_path and os.path.exists(config_path):
            self.load_config(config_path)
        else:
            self.config = self._get_default_config()
            self.logger.info("使用默认配置")

    def load_config(self, config_path: str) -> bool:
        """
        加载配置文件

        Args:
            config_path: 配置文件路径

        Returns:
            是否加载成功
        """
        try:
            if not os.path.exists(config_path):
                self.logger.error(f"配置文件不存在: {config_path}")
                return False

            file_ext = os.path.splitext(config_path)[1].lower()

            with open(config_path, 'r', encoding='utf-8') as f:
                if file_ext in ['.yml', '.yaml']:
                    self.config = yaml.safe_load(f)
                elif file_ext == '.json':
                    self.config = json.load(f)
                else:
                    self.logger.error(f"不支持的配置文件格式: {file_ext}")
                    return False

            self.config_path = config_path
            self.logger.info(f"配置文件加载成功: {config_path}")

            # 验证配置
            if self.validate_config():
                return True
            else:
                self.logger.error("配置验证失败，使用默认配置")
                self.config = self._get_default_config()
                return False

        except Exception as e:
            self.logger.error(f"加载配置文件失败: {str(e)}")
            self.config = self._get_default_config()
            return False

    def save_config(self, config_path: Optional[str] = None) -> bool:
        """
        保存配置到文件

        Args:
            config_path: 保存路径，如果为None则使用当前配置路径

        Returns:
            是否保存成功
        """
        save_path = config_path or self.config_path

        if not save_path:
            self.logger.error("未指定保存路径")
            return False

        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            file_ext = os.path.splitext(save_path)[1].lower()

            with open(save_path, 'w', encoding='utf-8') as f:
                if file_ext in ['.yml', '.yaml']:
                    yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True, indent=2)
                elif file_ext == '.json':
                    json.dump(self.config, f, indent=2, ensure_ascii=False)
                else:
                    self.logger.error(f"不支持的保存格式: {file_ext}")
                    return False

            self.logger.info(f"配置保存成功: {save_path}")
            return True

        except Exception as e:
            self.logger.error(f"保存配置失败: {str(e)}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值（支持点分隔的嵌套键）

        Args:
            key: 配置键，支持 'a.b.c' 格式
            default: 默认值

        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any):
        """
        设置配置值（支持点分隔的嵌套键）

        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config

        # 创建嵌套字典结构
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value
        self.logger.debug(f"配置更新: {key} = {value}")

    def update(self, updates: Dict[str, Any]):
        """
        批量更新配置

        Args:
            updates: 更新字典
        """
        for key, value in updates.items():
            self.set(key, value)

    def validate_config(self) -> bool:
        """
        验证配置有效性

        Returns:
            配置是否有效
        """
        try:
            # 验证必需的配置项
            required_sections = [
                'basic_filter',
                'main_strategy',
                'score_engine',
                'output'
            ]

            for section in required_sections:
                if section not in self.config:
                    self.logger.error(f"缺少必需的配置节: {section}")
                    return False

            # 验证基础过滤参数
            basic_filter = self.config.get('basic_filter', {})
            if not self._validate_numeric_range(basic_filter.get('max_price'), 0, 1000):
                self.logger.error("max_price 配置无效")
                return False

            if not self._validate_numeric_range(basic_filter.get('min_market_value'), 0, 1e12):
                self.logger.error("min_market_value 配置无效")
                return False

            if not self._validate_numeric_range(basic_filter.get('max_market_value'), 0, 1e12):
                self.logger.error("max_market_value 配置无效")
                return False

            # 验证评分引擎权重
            factors = self.config.get('score_engine', {}).get('factors', {})
            total_weight = 0
            for factor_name, factor_config in factors.items():
                if factor_config.get('enabled', True):
                    weight = factor_config.get('weight', 0)
                    if not self._validate_numeric_range(weight, 0, 1):
                        self.logger.error(f"因子 {factor_name} 权重配置无效: {weight}")
                        return False
                    total_weight += weight

            if abs(total_weight - 1.0) > 0.1:  # 允许10%的误差
                self.logger.warning(f"因子权重总和为 {total_weight}，建议接近1.0")

            self.logger.info("配置验证通过")
            return True

        except Exception as e:
            self.logger.error(f"配置验证失败: {str(e)}")
            return False

    def _validate_numeric_range(self, value: Any, min_val: float, max_val: float) -> bool:
        """验证数值范围"""
        if value is None:
            return False
        try:
            num_val = float(value)
            return min_val <= num_val <= max_val
        except (ValueError, TypeError):
            return False

    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """
        获取策略配置

        Args:
            strategy_name: 策略名称

        Returns:
            策略配置字典
        """
        return self.config.get(strategy_name, {})

    def get_factor_config(self, factor_name: str) -> Dict[str, Any]:
        """
        获取因子配置

        Args:
            factor_name: 因子名称

        Returns:
            因子配置字典
        """
        return self.config.get('score_engine', {}).get('factors', {}).get(factor_name, {})

    def _get_default_config(self) -> Dict[str, Any]:
        """
        获取默认配置

        Returns:
            默认配置字典
        """
        return {
            # 数据源配置
            "data_source": {
                "provider": "baostock",
                "cache_enabled": True,
                "cache_path": "./data/cache/"
            },

            # 基本筛选参数
            "basic_filter": {
                "max_price": 100.0,
                "min_market_value": 5000000000,  # 50亿
                "max_market_value": 50000000000,  # 500亿
                "min_volume": 1000
            },

            # 主策略参数
            "main_strategy": {
                "consecutive_days": 3,
                "max_cumulative_return": 0.05,  # 5%
                "turnover_strategy": "tiered",  # tiered/relative/activity
                "enable_market_filter": False,

                # 单日涨幅阈值（按市值分层）
                "small_cap_daily_return_threshold": 0.015,  # 1.5%
                "mid_cap_daily_return_threshold": 0.01,     # 1.0%
                "large_cap_daily_return_threshold": 0.005,  # 0.5%

                # 累计涨幅限制（按市值分层）
                "cumulative_return_limits": {
                    "small_cap": 0.08,   # 8%
                    "mid_cap": 0.05,     # 5%
                    "large_cap": 0.03    # 3%
                }
            },

            # 回退策略参数
            "fallback_strategy1": {
                "consecutive_days": 2,
                "return_limit_boost": 0.2  # 涨幅上限放宽20%
            },

            "fallback_strategy2": {
                "enable_simple_trend": True
            },

            # 兜底策略参数
            "emergency_strategy": {
                "max_candidates": 50,
                "emergency_max_price": 200.0,
                "emergency_min_volume": 100,
                "emergency_min_market_value": 1000000000,  # 10亿
                "emergency_max_market_value": 100000000000  # 1000亿
            },

            # 评分引擎配置
            "score_engine": {
                "normalization_method": "min_max",  # min_max/z_score/rank

                "factors": {
                    "financial": {
                        "weight": 0.30,
                        "enabled": True,
                        "normalization_method": "min_max"
                    },
                    "fund_flow": {
                        "weight": 0.25,
                        "enabled": True,
                        "normalization_method": "min_max"
                    },
                    "stability": {
                        "weight": 0.25,
                        "enabled": True,
                        "extreme_threshold": 0.05,  # 5%极端波动阈值
                        "normalization_method": "min_max"
                    },
                    "price_position": {
                        "weight": 0.20,
                        "enabled": True,
                        "lookback_period": 252,  # 252个交易日
                        "position_preference": "low",  # low/mid/high
                        "normalization_method": "min_max"
                    }
                }
            },

            # 输出配置
            "output": {
                "max_candidates": 20,
                "formats": ["csv", "json"],
                "output_path": "./results/",
                "include_visualization": False,
                "filename_template": "stock_candidates_{date}.{ext}"
            },

            # 日志配置
            "logging": {
                "level": "INFO",
                "file_path": "./logs/stock_selector.log",
                "max_file_size": "10MB",
                "backup_count": 5,
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },

            # 系统配置
            "system": {
                "max_workers": 4,  # 并行处理的最大工作线程数
                "timeout": 300,    # 超时时间（秒）
                "retry_count": 3   # 重试次数
            }
        }

    def get_config_schema(self) -> Dict[str, Any]:
        """
        获取配置文件的JSON Schema

        Returns:
            配置文件的JSON Schema
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Stock Selector Configuration",
            "type": "object",
            "properties": {
                "data_source": {
                    "type": "object",
                    "properties": {
                        "provider": {"type": "string", "enum": ["baostock"]},
                        "cache_enabled": {"type": "boolean"},
                        "cache_path": {"type": "string"}
                    },
                    "required": ["provider"]
                },
                "basic_filter": {
                    "type": "object",
                    "properties": {
                        "max_price": {"type": "number", "minimum": 0},
                        "min_market_value": {"type": "number", "minimum": 0},
                        "max_market_value": {"type": "number", "minimum": 0},
                        "min_volume": {"type": "number", "minimum": 0}
                    },
                    "required": ["max_price", "min_market_value", "max_market_value"]
                },
                "score_engine": {
                    "type": "object",
                    "properties": {
                        "factors": {
                            "type": "object",
                            "patternProperties": {
                                "^[a-zA-Z_]+$": {
                                    "type": "object",
                                    "properties": {
                                        "weight": {"type": "number", "minimum": 0, "maximum": 1},
                                        "enabled": {"type": "boolean"}
                                    },
                                    "required": ["weight", "enabled"]
                                }
                            }
                        }
                    }
                }
            },
            "required": ["basic_filter", "score_engine"]
        }

    def export_config_template(self, output_path: str) -> bool:
        """
        导出配置模板文件

        Args:
            output_path: 输出路径

        Returns:
            是否导出成功
        """
        try:
            template_config = self._get_default_config()

            # 添加注释说明
            template_config["_description"] = {
                "version": "1.0",
                "created": datetime.now().isoformat(),
                "description": "股票选择策略系统配置文件模板",
                "sections": {
                    "data_source": "数据源配置",
                    "basic_filter": "基础筛选参数",
                    "main_strategy": "主策略参数",
                    "score_engine": "评分引擎配置",
                    "output": "输出配置",
                    "logging": "日志配置"
                }
            }

            file_ext = os.path.splitext(output_path)[1].lower()

            with open(output_path, 'w', encoding='utf-8') as f:
                if file_ext in ['.yml', '.yaml']:
                    yaml.dump(template_config, f, default_flow_style=False, allow_unicode=True, indent=2)
                elif file_ext == '.json':
                    json.dump(template_config, f, indent=2, ensure_ascii=False)

            self.logger.info(f"配置模板导出成功: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"导出配置模板失败: {str(e)}")
            return False

    def __str__(self) -> str:
        """字符串表示"""
        return f"ConfigManager(config_path={self.config_path}, sections={list(self.config.keys())})"