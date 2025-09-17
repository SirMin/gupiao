import time
from abc import ABC, abstractmethod

class StockDataSource(ABC):
    def __init__(self, cooldown=60, max_fail=3):
        """
        :param cooldown: 冷却秒数（连续失败后暂停多久）
        :param max_fail: 最大失败次数，超过后触发冷却
        """
        self.fail_count = 0
        self.cooldown_until = 0
        self.cooldown = cooldown
        self.max_fail = max_fail

    def record_success(self):
        """请求成功，清除失败状态"""
        self.fail_count = 0
        self.cooldown_until = 0

    def record_failure(self, reason: str = None):
        """记录一次失败，并决定是否进入冷却"""
        self.fail_count += 1
        if self.fail_count >= self.max_fail:
            self.cooldown_until = time.time() + self.cooldown
            print(f"[COOLDOWN] {self.__class__.__name__} 进入冷却 {self.cooldown}s, 原因: {reason}")

    def is_available(self) -> bool:
        """检查当前是否可用"""
        return time.time() >= self.cooldown_until

    @abstractmethod
    def get_stock_list(self): pass

    @abstractmethod
    def get_daily(self, code, start_date, end_date=None, adjust="qfq"): pass

    @abstractmethod
    def get_today(self, code): pass