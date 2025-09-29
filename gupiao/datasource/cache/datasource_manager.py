import time
import threading
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import random

from datasource.stock.base import StockDataSource, StockDataResult



class LoadBalanceStrategy(Enum):
    """负载均衡策略"""

    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    PRIORITY_FIRST = "priority_first"
    RANDOM = "random"
    RESPONSE_TIME = "response_time"


class CircuitBreakerState(Enum):
    """熔断器状态"""

    CLOSED = "closed"  # 正常状态
    OPEN = "open"  # 熔断状态
    HALF_OPEN = "half_open"  # 半开状态


@dataclass
class DataSourceInfo:
    """数据源信息"""

    datasource: StockDataSource  # StockDataSource实例
    priority: int = 0
    weight: int = 1
    total_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    last_error: Optional[str] = None
    last_error_time: Optional[float] = None
    circuit_breaker_state: CircuitBreakerState = CircuitBreakerState.CLOSED
    circuit_breaker_failure_count: int = 0
    circuit_breaker_last_failure_time: Optional[float] = None
    enabled: bool = True

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 1.0
        return (self.total_requests - self.failed_requests) / self.total_requests

    @property
    def avg_response_time(self) -> float:
        """平均响应时间"""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests

    @property
    def name(self) -> str:
        """数据源名称"""
        return getattr(self.datasource, "__class__", type(self.datasource)).__name__


class DataSourceManager:
    """多数据源管理器"""

    def __init__(
        self,
        datasources: Optional[List[Any]] = None,
        load_balance_strategy: LoadBalanceStrategy = LoadBalanceStrategy.PRIORITY_FIRST,
        failover_enabled: bool = True,
        circuit_breaker_enabled: bool = True,
        health_check_enabled: bool = True,
        **config,
    ):
        """
        初始化多数据源管理器

        Args:
            datasources: 数据源列表
            load_balance_strategy: 负载均衡策略
            failover_enabled: 是否启用故障转移
            circuit_breaker_enabled: 是否启用熔断器
            health_check_enabled: 是否启用健康检查
            **config: 其他配置参数
        """
        self.datasources: Dict[str, DataSourceInfo] = {}
        self.load_balance_strategy = load_balance_strategy
        self.failover_enabled = failover_enabled
        self.circuit_breaker_enabled = circuit_breaker_enabled
        self.health_check_enabled = health_check_enabled

        # 配置参数
        self.config = {
            "retry_count": 3,
            "retry_delay": 1.0,
            "circuit_breaker_threshold": 5,
            "circuit_breaker_timeout": 60,
            "health_check_interval": 30,
            "request_timeout": 30,
            "max_concurrent_requests": 5,
            **config,
        }

        # 状态管理
        self._lock = threading.RLock()
        self._round_robin_index = 0
        self._concurrent_requests = 0
        self._last_health_check = 0

        # 添加初始数据源
        if datasources:
            for i, ds in enumerate(datasources):
                self.add_datasource(ds, priority=i)

    def add_datasource(
        self, datasource: Any, priority: int = 0, weight: int = 1
    ) -> None:
        """
        添加数据源

        Args:
            datasource: 数据源实例
            priority: 优先级（数字越小优先级越高）
            weight: 权重
        """
        with self._lock:
            ds_info = DataSourceInfo(
                datasource=datasource, priority=priority, weight=weight
            )
            # 生成唯一的数据源名称
            base_name = ds_info.name
            ds_name = base_name
            counter = 1
            while ds_name in self.datasources:
                ds_name = f"{base_name}_{counter}"
                counter += 1

            self.datasources[ds_name] = ds_info

    def remove_datasource(self, datasource: Any) -> None:
        """
        移除数据源

        Args:
            datasource: 数据源实例
        """
        with self._lock:
            # 找到匹配的数据源并删除
            ds_name_to_remove = None
            for ds_name, ds_info in self.datasources.items():
                if ds_info.datasource is datasource:
                    ds_name_to_remove = ds_name
                    break

            if ds_name_to_remove:
                del self.datasources[ds_name_to_remove]

    def execute_query(self, method_name: str, *args, **kwargs) -> StockDataResult:
        """
        执行查询

        Args:
            method_name: 方法名
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            Any: 查询结果

        Raises:
            Exception: 所有数据源都失败时抛出异常
        """
        # 检查并发限制
        with self._lock:
            if self._concurrent_requests >= self.config["max_concurrent_requests"]:
                raise Exception("达到最大并发请求数限制")
            self._concurrent_requests += 1

        try:
            # 定期健康检查
            self._periodic_health_check()

            # 获取可用数据源
            available_datasources = self._get_available_datasources()
            if not available_datasources:
                raise Exception("没有可用的数据源")

            # 按策略选择数据源
            ordered_datasources = self._order_datasources(available_datasources)

            last_exception = None

            for ds_info in ordered_datasources:
                try:
                    result = self._execute_single_query(
                        ds_info, method_name, *args, **kwargs
                    )
                    return result

                except Exception as e:
                    last_exception = e
                    self._record_failure(ds_info, str(e))

                    if not self.failover_enabled:
                        raise e

                    # 继续尝试下一个数据源
                    continue

            # 所有数据源都失败
            raise Exception(f"所有数据源都失败，最后错误: {last_exception}")

        finally:
            with self._lock:
                self._concurrent_requests -= 1

    def get_healthy_datasources(self) -> List[str]:
        """
        获取健康的数据源名称列表

        Returns:
            List[str]: 健康数据源名称列表
        """
        with self._lock:
            healthy = []
            for name, ds_info in self.datasources.items():
                if (
                    ds_info.enabled
                    and ds_info.circuit_breaker_state != CircuitBreakerState.OPEN
                    and ds_info.success_rate > 0.5
                ):
                    healthy.append(name)
            return healthy

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._lock:
            stats = {}
            for name, ds_info in self.datasources.items():
                stats[name] = {
                    "priority": ds_info.priority,
                    "weight": ds_info.weight,
                    "enabled": ds_info.enabled,
                    "total_requests": ds_info.total_requests,
                    "failed_requests": ds_info.failed_requests,
                    "success_rate": ds_info.success_rate,
                    "avg_response_time": ds_info.avg_response_time,
                    "circuit_breaker_state": ds_info.circuit_breaker_state.value,
                    "last_error": ds_info.last_error,
                    "last_error_time": ds_info.last_error_time,
                }
            return stats

    def check_datasource_health(self, datasource: Any) -> bool:
        """
        检查数据源健康状态

        Args:
            datasource: 数据源实例

        Returns:
            bool: 是否健康
        """
        try:
            # 这里可以实现具体的健康检查逻辑
            # 比如调用数据源的健康检查方法
            if hasattr(datasource, "health_check"):
                return datasource.health_check()

            # 简单的可用性检查
            return hasattr(datasource, "query_history_k_data_plus")

        except Exception:
            return False

    def enable_datasource(self, name: str) -> None:
        """启用数据源"""
        with self._lock:
            if name in self.datasources:
                self.datasources[name].enabled = True

    def disable_datasource(self, name: str) -> None:
        """禁用数据源"""
        with self._lock:
            if name in self.datasources:
                self.datasources[name].enabled = False

    def reset_circuit_breaker(self, name: str) -> None:
        """重置熔断器"""
        with self._lock:
            if name in self.datasources:
                ds_info = self.datasources[name]
                ds_info.circuit_breaker_state = CircuitBreakerState.CLOSED
                ds_info.circuit_breaker_failure_count = 0
                ds_info.circuit_breaker_last_failure_time = None

    def _get_available_datasources(self) -> List[DataSourceInfo]:
        """获取可用数据源列表"""
        available = []
        current_time = time.time()

        for ds_info in self.datasources.values():
            if not ds_info.enabled:
                continue

            # 检查熔断器状态
            if self.circuit_breaker_enabled:
                if ds_info.circuit_breaker_state == CircuitBreakerState.OPEN:
                    # 检查是否可以转为半开状态
                    if (
                        ds_info.circuit_breaker_last_failure_time
                        and current_time - ds_info.circuit_breaker_last_failure_time
                        > self.config["circuit_breaker_timeout"]
                    ):
                        ds_info.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                        ds_info.circuit_breaker_failure_count = 0
                    else:
                        continue

            available.append(ds_info)

        return available

    def _order_datasources(
        self, datasources: List[DataSourceInfo]
    ) -> List[DataSourceInfo]:
        """根据负载均衡策略排序数据源"""
        if self.load_balance_strategy == LoadBalanceStrategy.PRIORITY_FIRST:
            return sorted(datasources, key=lambda x: x.priority)

        elif self.load_balance_strategy == LoadBalanceStrategy.RESPONSE_TIME:
            return sorted(datasources, key=lambda x: x.avg_response_time)

        elif self.load_balance_strategy == LoadBalanceStrategy.ROUND_ROBIN:
            if datasources:
                self._round_robin_index = (self._round_robin_index + 1) % len(
                    datasources
                )
                return (
                    datasources[self._round_robin_index :]
                    + datasources[: self._round_robin_index]
                )

        elif self.load_balance_strategy == LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN:
            # 简化的加权轮询实现
            weighted_list = []
            for ds_info in datasources:
                weighted_list.extend([ds_info] * ds_info.weight)
            if weighted_list:
                self._round_robin_index = (self._round_robin_index + 1) % len(
                    weighted_list
                )
                selected = weighted_list[self._round_robin_index]
                # 将选中的数据源放在第一位
                return [selected] + [ds for ds in datasources if ds != selected]

        elif self.load_balance_strategy == LoadBalanceStrategy.RANDOM:
            return random.sample(datasources, len(datasources))

        return datasources

    def _execute_single_query(
        self, ds_info: DataSourceInfo, method_name: str, *args, **kwargs
    ) -> Any:
        """执行单个数据源的查询"""
        start_time = time.time()

        try:
            # 获取方法
            method = getattr(ds_info.datasource, method_name)
            if not callable(method):
                raise AttributeError(f"方法 {method_name} 不存在或不可调用")

            # 执行查询
            result = method(*args, **kwargs)

            # 记录成功
            end_time = time.time()
            response_time = end_time - start_time

            with self._lock:
                ds_info.total_requests += 1
                ds_info.total_response_time += response_time

                # 重置熔断器状态
                if ds_info.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
                    ds_info.circuit_breaker_state = CircuitBreakerState.CLOSED
                    ds_info.circuit_breaker_failure_count = 0

            return result

        except Exception as e:
            # 记录失败
            end_time = time.time()
            response_time = end_time - start_time

            with self._lock:
                ds_info.total_requests += 1
                ds_info.total_response_time += response_time

            raise e

    def _record_failure(self, ds_info: DataSourceInfo, error_msg: str) -> None:
        """记录失败"""
        current_time = time.time()

        with self._lock:
            ds_info.failed_requests += 1
            ds_info.last_error = error_msg
            ds_info.last_error_time = current_time

            # 更新熔断器状态
            if self.circuit_breaker_enabled:
                ds_info.circuit_breaker_failure_count += 1
                ds_info.circuit_breaker_last_failure_time = current_time

                # 检查是否需要触发熔断器
                if (
                    ds_info.circuit_breaker_failure_count
                    >= self.config["circuit_breaker_threshold"]
                ):
                    ds_info.circuit_breaker_state = CircuitBreakerState.OPEN

    def _periodic_health_check(self) -> None:
        """定期健康检查"""
        if not self.health_check_enabled:
            return

        current_time = time.time()
        if (
            current_time - self._last_health_check
            < self.config["health_check_interval"]
        ):
            return

        self._last_health_check = current_time

        # 在后台线程中执行健康检查
        def health_check_task():
            with self._lock:
                for name, ds_info in self.datasources.items():
                    try:
                        is_healthy = self.check_datasource_health(ds_info.datasource)
                        if not is_healthy and ds_info.enabled:
                            print(f"数据源 {name} 健康检查失败，暂时禁用")
                            ds_info.enabled = False
                        elif is_healthy and not ds_info.enabled:
                            print(f"数据源 {name} 健康检查恢复，重新启用")
                            ds_info.enabled = True
                            self.reset_circuit_breaker(name)
                    except Exception as e:
                        print(f"数据源 {name} 健康检查异常: {e}")

        # 启动后台线程
        threading.Thread(target=health_check_task, daemon=True).start()
