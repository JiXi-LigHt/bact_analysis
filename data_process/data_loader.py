import os
import pandas as pd
from typing import Optional


class DataLoader:
    def __init__(self):
        """
        初始化一个无主键、基于整行内容去重的 DataLoader。
        所有列参与比较，仅当整行完全相同时才视为重复。
        """
        self.data = pd.DataFrame()

    def _read_file(self, file_path: str) -> pd.DataFrame:
        """根据扩展名读取 CSV 或 Excel 文件"""
        _, ext = os.path.splitext(file_path.lower())
        if ext == '.csv':
            return pd.read_csv(file_path)
        elif ext in ('.xls', '.xlsx'):
            return pd.read_excel(file_path)
        else:
            raise ValueError(f"不支持的文件格式: {ext}")

    def load(self, file_path: str, incremental: bool = True) -> pd.DataFrame:
        """
        加载数据并去重合并。

        :param file_path: 文件路径
        :param incremental: 是否增量更新（默认 True）
        :return: 去重后的完整数据
        """
        new_data = self._read_file(file_path)

        if incremental and not self.data.empty:
            # 拼接旧数据和新数据
            combined = pd.concat([self.data, new_data], ignore_index=True)
        else:
            combined = new_data

        # 基于整行内容去重，保留最后一次出现的（新数据优先）
        self.data = combined.drop_duplicates(keep='last').reset_index(drop=True)
        return self.data.copy()

    def get_data(self) -> pd.DataFrame:
        return self.data.copy()

    def clear(self):
        self.data = pd.DataFrame()