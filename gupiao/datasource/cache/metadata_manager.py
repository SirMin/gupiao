import json
import os
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime
from .range_calculator import DateRange, RangeCalculator


class MetadataManager:
    """元数据管理器"""

    def __init__(self, cache_dir: str, backup_interval: int = 100):
        """
        初始化元数据管理器

        Args:
            cache_dir: 缓存目录
            backup_interval: 元数据备份间隔（操作次数）
        """
        self.cache_dir = cache_dir
        self.backup_interval = backup_interval
        self.metadata_file = os.path.join(cache_dir, 'metadata.json')
        self.backup_file = os.path.join(cache_dir, 'metadata_backup.json')

        # 内存中的元数据
        self._metadata: Dict[str, Dict[str, Any]] = {}

        # 线程锁，确保线程安全
        self._lock = threading.RLock()

        # 操作计数器，用于定期备份
        self._operation_count = 0

        # 确保目录存在
        os.makedirs(cache_dir, exist_ok=True)

        # 加载现有元数据
        self._load_metadata()

    def get_cached_ranges(self, query_key: str) -> List[DateRange]:
        """
        获取指定查询键的已缓存日期范围

        Args:
            query_key: 查询键

        Returns:
            List[DateRange]: 已缓存的日期范围列表
        """
        with self._lock:
            if query_key not in self._metadata:
                return []

            ranges_data = self._metadata[query_key].get('cached_ranges', [])
            return [DateRange(r['start'], r['end']) for r in ranges_data]

    def update_cached_ranges(self, query_key: str, new_ranges: List[DateRange], fields: List[str]) -> None:
        """
        更新指定查询键的缓存范围

        Args:
            query_key: 查询键
            new_ranges: 新的日期范围列表
            fields: 字段列表
        """
        with self._lock:
            # 获取现有范围
            existing_ranges = self.get_cached_ranges(query_key)

            # 合并新范围
            all_ranges = existing_ranges + new_ranges
            merged_ranges = RangeCalculator.merge_ranges(all_ranges)

            # 更新元数据
            self._metadata[query_key] = {
                'cached_ranges': [
                    {'start': r.start, 'end': r.end}
                    for r in merged_ranges
                ],
                'last_updated': datetime.now().isoformat(),
                'fields': fields,
                'total_records': self._calculate_total_records(query_key, merged_ranges)
            }

            self._increment_operations()

    def remove_cached_range(self, query_key: str, range_to_remove: DateRange) -> None:
        """
        移除指定查询键的某个缓存范围

        Args:
            query_key: 查询键
            range_to_remove: 要移除的日期范围
        """
        with self._lock:
            if query_key not in self._metadata:
                return

            existing_ranges = self.get_cached_ranges(query_key)
            updated_ranges = []

            for existing_range in existing_ranges:
                if not existing_range.overlaps(range_to_remove):
                    # 没有重叠，保留
                    updated_ranges.append(existing_range)
                else:
                    # 有重叠，计算剩余部分
                    if existing_range.start < range_to_remove.start:
                        # 前半部分
                        end_date = RangeCalculator._previous_date(range_to_remove.start)
                        if existing_range.start <= end_date:
                            updated_ranges.append(DateRange(existing_range.start, end_date))

                    if existing_range.end > range_to_remove.end:
                        # 后半部分
                        start_date = RangeCalculator._next_date(range_to_remove.end)
                        if start_date <= existing_range.end:
                            updated_ranges.append(DateRange(start_date, existing_range.end))

            # 更新元数据
            if updated_ranges:
                fields = self._metadata[query_key].get('fields', [])
                self._metadata[query_key] = {
                    'cached_ranges': [
                        {'start': r.start, 'end': r.end}
                        for r in updated_ranges
                    ],
                    'last_updated': datetime.now().isoformat(),
                    'fields': fields,
                    'total_records': self._calculate_total_records(query_key, updated_ranges)
                }
            else:
                # 没有剩余范围，删除整个键
                del self._metadata[query_key]

            self._increment_operations()

    def clear_query_key(self, query_key: str) -> None:
        """
        清除指定查询键的所有元数据

        Args:
            query_key: 查询键
        """
        with self._lock:
            if query_key in self._metadata:
                del self._metadata[query_key]
                self._increment_operations()

    def get_query_info(self, query_key: str) -> Optional[Dict[str, Any]]:
        """
        获取查询键的详细信息

        Args:
            query_key: 查询键

        Returns:
            Optional[Dict[str, Any]]: 查询信息，不存在时返回None
        """
        with self._lock:
            return self._metadata.get(query_key)

    def get_all_query_keys(self) -> List[str]:
        """
        获取所有查询键

        Returns:
            List[str]: 所有查询键列表
        """
        with self._lock:
            return list(self._metadata.keys())

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取元数据统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        with self._lock:
            total_queries = len(self._metadata)
            total_ranges = sum(
                len(info.get('cached_ranges', []))
                for info in self._metadata.values()
            )
            total_records = sum(
                info.get('total_records', 0)
                for info in self._metadata.values()
            )

            # 计算日期覆盖范围
            all_ranges = []
            for info in self._metadata.values():
                ranges_data = info.get('cached_ranges', [])
                all_ranges.extend([DateRange(r['start'], r['end']) for r in ranges_data])

            start_date, end_date = RangeCalculator.get_date_boundaries(all_ranges)

            return {
                'total_query_keys': total_queries,
                'total_cached_ranges': total_ranges,
                'total_records': total_records,
                'date_coverage': {
                    'start': start_date,
                    'end': end_date
                },
                'metadata_file_size': self._get_metadata_file_size(),
                'last_backup_time': self._get_last_backup_time()
            }

    def cleanup_expired_metadata(self, retention_days: int = 365) -> int:
        """
        清理过期的元数据

        Args:
            retention_days: 保留天数

        Returns:
            int: 清理的查询键数量
        """
        with self._lock:
            cutoff_date = datetime.now().timestamp() - (retention_days * 24 * 3600)
            expired_keys = []

            for query_key, info in self._metadata.items():
                last_updated_str = info.get('last_updated')
                if last_updated_str:
                    try:
                        last_updated = datetime.fromisoformat(last_updated_str).timestamp()
                        if last_updated < cutoff_date:
                            expired_keys.append(query_key)
                    except ValueError:
                        # 如果日期格式有问题，也认为是过期的
                        expired_keys.append(query_key)

            # 删除过期键
            for key in expired_keys:
                del self._metadata[key]

            if expired_keys:
                self._increment_operations()

            return len(expired_keys)

    def validate_metadata(self) -> List[str]:
        """
        验证元数据的完整性

        Returns:
            List[str]: 发现的问题列表
        """
        issues = []

        with self._lock:
            for query_key, info in self._metadata.items():
                # 检查必需字段
                required_fields = ['cached_ranges', 'last_updated', 'fields']
                for field in required_fields:
                    if field not in info:
                        issues.append(f"查询键 {query_key} 缺少字段: {field}")

                # 检查日期范围格式
                ranges_data = info.get('cached_ranges', [])
                for i, range_data in enumerate(ranges_data):
                    if 'start' not in range_data or 'end' not in range_data:
                        issues.append(f"查询键 {query_key} 范围 {i} 缺少start或end字段")
                        continue

                    try:
                        datetime.strptime(range_data['start'], '%Y-%m-%d')
                        datetime.strptime(range_data['end'], '%Y-%m-%d')
                    except ValueError:
                        issues.append(f"查询键 {query_key} 范围 {i} 日期格式错误")

                    if range_data['start'] > range_data['end']:
                        issues.append(f"查询键 {query_key} 范围 {i} 开始日期晚于结束日期")

        return issues

    def repair_metadata(self) -> Dict[str, int]:
        """
        修复元数据中的问题

        Returns:
            Dict[str, int]: 修复统计信息
        """
        stats = {
            'removed_invalid_keys': 0,
            'merged_overlapping_ranges': 0,
            'fixed_date_formats': 0
        }

        with self._lock:
            keys_to_remove = []

            for query_key, info in list(self._metadata.items()):
                try:
                    # 验证并修复日期范围
                    ranges_data = info.get('cached_ranges', [])
                    valid_ranges = []

                    for range_data in ranges_data:
                        if 'start' in range_data and 'end' in range_data:
                            try:
                                # 验证日期格式
                                start_date = datetime.strptime(range_data['start'], '%Y-%m-%d')
                                end_date = datetime.strptime(range_data['end'], '%Y-%m-%d')

                                if start_date <= end_date:
                                    valid_ranges.append(DateRange(range_data['start'], range_data['end']))
                                else:
                                    stats['fixed_date_formats'] += 1
                            except ValueError:
                                stats['fixed_date_formats'] += 1

                    if valid_ranges:
                        # 合并重叠范围
                        original_count = len(valid_ranges)
                        merged_ranges = RangeCalculator.merge_ranges(valid_ranges)
                        if len(merged_ranges) < original_count:
                            stats['merged_overlapping_ranges'] += 1

                        # 更新元数据
                        info['cached_ranges'] = [
                            {'start': r.start, 'end': r.end}
                            for r in merged_ranges
                        ]
                        info['total_records'] = self._calculate_total_records(query_key, merged_ranges)
                    else:
                        keys_to_remove.append(query_key)

                except Exception:
                    keys_to_remove.append(query_key)

            # 移除无效的键
            for key in keys_to_remove:
                del self._metadata[key]
                stats['removed_invalid_keys'] += 1

            if keys_to_remove or any(stats.values()):
                self._increment_operations()

        return stats

    def save_metadata(self) -> bool:
        """
        保存元数据到文件

        Returns:
            bool: 保存是否成功
        """
        try:
            with self._lock:
                # 创建备份
                if os.path.exists(self.metadata_file):
                    import shutil
                    shutil.copy2(self.metadata_file, self.backup_file)

                # 保存新数据
                with open(self.metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(self._metadata, f, indent=2, ensure_ascii=False)

                return True

        except Exception as e:
            print(f"保存元数据失败: {e}")
            return False

    def _load_metadata(self) -> None:
        """从文件加载元数据"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    self._metadata = json.load(f)
            elif os.path.exists(self.backup_file):
                # 主文件不存在，尝试从备份恢复
                print("主元数据文件不存在，从备份恢复...")
                with open(self.backup_file, 'r', encoding='utf-8') as f:
                    self._metadata = json.load(f)
                # 立即保存到主文件
                self.save_metadata()
            else:
                self._metadata = {}

        except Exception as e:
            print(f"加载元数据失败: {e}")
            self._metadata = {}

    def _increment_operations(self) -> None:
        """增加操作计数并检查是否需要备份"""
        self._operation_count += 1
        if self._operation_count >= self.backup_interval:
            self.save_metadata()
            self._operation_count = 0

    def _calculate_total_records(self, query_key: str, ranges: List[DateRange]) -> int:
        """
        估算记录总数（基于日期范围）

        Args:
            query_key: 查询键
            ranges: 日期范围列表

        Returns:
            int: 估算的记录总数
        """
        # 简单估算：每个交易日一条记录
        total_days = 0
        for date_range in ranges:
            trading_dates = RangeCalculator.get_trading_dates_in_range(
                date_range.start, date_range.end
            )
            total_days += len(trading_dates)

        return total_days

    def _get_metadata_file_size(self) -> int:
        """获取元数据文件大小"""
        try:
            if os.path.exists(self.metadata_file):
                return os.path.getsize(self.metadata_file)
            return 0
        except Exception:
            return 0

    def _get_last_backup_time(self) -> Optional[str]:
        """获取最后备份时间"""
        try:
            if os.path.exists(self.backup_file):
                mtime = os.path.getmtime(self.backup_file)
                return datetime.fromtimestamp(mtime).isoformat()
            return None
        except Exception:
            return None