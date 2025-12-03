import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams

# --- 设置通用中文支持 ---
rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
rcParams['axes.unicode_minus'] = False


class VisualResistanceMonitor:
    def __init__(self, df_res, df_cnt):
        self.df_res = df_res.copy()
        self.df_cnt = df_cnt.copy()
        self._preprocess()

    def _preprocess(self):
        """数据预处理"""
        self.df_res['datetime'] = pd.to_datetime(self.df_res['datetime'])
        self.df_cnt['date'] = pd.to_datetime(self.df_cnt['date'])

        # 归一化日期到“天”用于关联
        self.df_res['date'] = self.df_res['datetime'].dt.floor('D')

        # 数值转换
        self.df_res['resistance_rate'] = pd.to_numeric(self.df_res['resistance_rate'], errors='coerce')
        self.df_cnt['daily_count'] = pd.to_numeric(self.df_cnt['daily_count'], errors='coerce').fillna(0)

    def _analyze_single_group(self, group_res, group_cnt, location, bacteria, window_days, z_threshold):
        """
        核心算法：计算每一个点的异常情况
        注：这里接收全量数据进行计算，以保证起始日期的基线是准确的
        """
        # --- 1. 每日样本量分析 (Daily Count) ---
        if group_cnt.empty:
            # 构造空表结构以免后续merge报错
            df_cnt_analysis = pd.DataFrame(columns=['date', 'daily_count', 'pred_count', 'z_cnt', 'is_alert_cnt'])
        else:
            min_date = group_cnt['date'].min()
            # 确保时间轴覆盖所有数据
            max_date = max(group_cnt['date'].max(),
                           group_res['date'].max() if not group_res.empty else group_cnt['date'].max())
            full_idx = pd.date_range(start=min_date, end=max_date, freq='D')

            ts_cnt = group_cnt.set_index('date')['daily_count'].reindex(full_idx, fill_value=0).to_frame()

            # 滚动计算 (包含当前点之前的N天)
            roll_cnt = ts_cnt['daily_count'].shift(1).rolling(window=window_days, min_periods=1)
            ts_cnt['pred_count'] = roll_cnt.mean()
            std_cnt = roll_cnt.std().replace(0, 1e-6)

            ts_cnt['z_cnt'] = (ts_cnt['daily_count'] - ts_cnt['pred_count']) / std_cnt
            ts_cnt['is_alert_cnt'] = (ts_cnt['z_cnt'] > z_threshold) & (ts_cnt['daily_count'] > 2)

            df_cnt_analysis = ts_cnt.reset_index().rename(columns={'index': 'date'})

        # --- 2. 离散耐药率分析 (Discrete Resistance Rate) ---
        if group_res.empty:
            return pd.DataFrame()

        df_res_discrete = group_res.sort_values('datetime').copy()
        df_res_discrete = df_res_discrete.set_index('datetime')

        # Time-aware Rolling:
        # window='7D' (物理时间7天), closed='left' (不包含当前点，纯预测)
        # 如果pandas版本较低不支持closed='left'，可使用默认并忽略微小偏差
        try:
            indexer = df_res_discrete['resistance_rate'].rolling(window=f'{window_days}D', closed='left', min_periods=1)
            df_res_discrete['pred_res'] = indexer.mean()
            df_res_discrete['std_res'] = indexer.std().replace(0, 1e-6)
        except:
            # 兼容旧版Pandas
            indexer = df_res_discrete['resistance_rate'].rolling(window=f'{window_days}D', min_periods=1)
            df_res_discrete['pred_res'] = indexer.mean()
            df_res_discrete['std_res'] = indexer.std().replace(0, 1e-6)

        # 计算离群程度
        df_res_discrete['z_res'] = (df_res_discrete['resistance_rate'] - df_res_discrete['pred_res']) / df_res_discrete[
            'std_res']
        df_res_discrete['is_alert_res'] = df_res_discrete['z_res'] > z_threshold

        df_res_discrete = df_res_discrete.reset_index()

        # --- 3. 合并数据 ---
        final_df = pd.merge(df_res_discrete,
                            df_cnt_analysis[['date', 'daily_count', 'pred_count', 'is_alert_cnt']],
                            on='date',
                            how='left')

        final_df['hospital_location'] = location
        final_df['micro_test_name'] = bacteria

        return final_df

    def run_analysis(self,
                     window=7,
                     z_threshold=2.5,
                     start_date=None,
                     end_date=None,
                     target_locations=None,
                     target_bacteria=None):
        """
        执行全量分析，并支持日期筛选

        Parameters:
        -----------
        window : int
            滚动窗口天数
        z_threshold : float
            异常判定阈值 (Z-Score)
        start_date : str or datetime, optional
            分析开始日期 (e.g. '2023-01-01')，默认为None(不限制)
        end_date : str or datetime, optional
            分析结束日期 (e.g. '2023-12-31')，默认为None(不限制)
        target_locations  : list or str, optional
            指定要分析的院区/科室列表。如 None 则分析所有。
        target_bacteria : list or str, optional
            指定要分析的细菌列表。如 None 则分析所有。
        """
        results = []

        # --- 参数标准化 (支持传入单个字符串) ---
        if isinstance(target_locations, str):
            target_locations = [target_locations]
        if isinstance(target_bacteria, str):
            target_bacteria = [target_bacteria]

        if not target_locations:
            target_locations = None
        if not target_bacteria:
            target_bacteria = None

        # 处理日期格式
        s_date = pd.to_datetime(start_date) if start_date else None
        e_date = pd.to_datetime(end_date) if end_date else None

        # 获取所有组合
        combos = self.df_res[['hospital_location', 'micro_test_name']].drop_duplicates().values
        for loc, bact in combos:
            # 筛选逻辑：如果指定了列表且当前项不在列表中，则跳过
            if target_locations is not None and loc not in target_locations:
                continue
            if target_bacteria is not None and bact not in target_bacteria:
                continue

            # 提取数据子集
            sub_res = self.df_res[(self.df_res['hospital_location'] == loc) & (self.df_res['micro_test_name'] == bact)]
            sub_cnt = self.df_cnt[(self.df_cnt['hospital_location'] == loc) & (self.df_cnt['micro_test_name'] == bact)]

            # 1. 先进行全量计算 (保证窗口函数能取到start_date之前的历史数据作为基线)
            df_analyzed = self._analyze_single_group(sub_res, sub_cnt, loc, bact, window, z_threshold)

            if df_analyzed.empty:
                continue

            # 2. 计算完成后，再根据日期范围进行过滤 (Filter Results)
            if s_date:
                df_analyzed = df_analyzed[df_analyzed['date'] >= s_date]
            if e_date:
                df_analyzed = df_analyzed[df_analyzed['date'] <= e_date]

            # 3. 如果过滤后还有数据，则保留
            if not df_analyzed.empty:
                results.append(df_analyzed)

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    def plot_anomalies(self, df_result):
        """
        可视化功能：绘制过滤后的异常数据
        """
        if df_result.empty:
            print("当前日期范围内无数据或无异常。")
            return

        # 筛选出有异常的组（Count异常 或 Resistance异常）
        anomaly_groups = df_result[df_result['is_alert_cnt'] | df_result['is_alert_res']]
        target_combos = anomaly_groups[['hospital_location', 'micro_test_name']].drop_duplicates().values

        if len(target_combos) == 0:
            # st.success("✅ 未检测到任何异常事件，一切正常。")
            print("✅ 未检测到任何异常事件，一切正常。")
            return

        # st.markdown(f"### 🚨 检测到 {len(target_combos)} 个异常事件")
        print(f"### 🚨 检测到 {len(target_combos)} 个异常事件")

        for loc, bact in target_combos:

            data = df_result[(df_result['hospital_location'] == loc) &
                             (df_result['micro_test_name'] == bact)].sort_values('datetime')

            if data.empty: continue

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, gridspec_kw={'height_ratios': [1, 2]})

            # 动态标题，显示日期范围
            date_min_str = data['date'].min().strftime('%Y-%m-%d')
            date_max_str = data['date'].max().strftime('%Y-%m-%d')
            fig.suptitle(f'异常监测: {loc} - {bact}\n({date_min_str} 至 {date_max_str})', fontsize=14,
                         fontweight='bold')

            # --- Plot 1: 每日样本量 ---
            # 去重获取每日统计数据
            daily_data = data[['date', 'daily_count', 'pred_count', 'is_alert_cnt']].drop_duplicates(
                'date').sort_values('date')

            ax1.bar(daily_data['date'], daily_data['daily_count'], color='#e0e0e0', width=0.8, label='每日检出数')
            ax1.plot(daily_data['date'], daily_data['pred_count'], color='orange', linestyle='--', label='基线')

            cnt_alerts = daily_data[daily_data['is_alert_cnt']]
            if not cnt_alerts.empty:
                ax1.scatter(cnt_alerts['date'], cnt_alerts['daily_count'], color='red', marker='v', zorder=5,
                            label='爆发预警')

            ax1.set_ylabel('样本量')
            ax1.legend(loc='upper left', fontsize='small')
            ax1.grid(True, linestyle=':', alpha=0.3)

            # --- Plot 2: 离散耐药率 ---
            # 历史基线
            ax2.plot(data['datetime'], data['pred_res'], color='green', alpha=0.4, linestyle='-', label='预测基线')

            # 正常点
            normal_points = data[~data['is_alert_res']]
            ax2.scatter(normal_points['datetime'], normal_points['resistance_rate'],
                        c='gray', alpha=0.5, s=30, label='正常检测')

            # 异常点
            alert_points = data[data['is_alert_res']]
            if not alert_points.empty:
                ax2.scatter(alert_points['datetime'], alert_points['resistance_rate'],
                            c='red', s=80, edgecolors='black', zorder=10, label='耐药异常')

                for _, row in alert_points.iterrows():
                    ax2.annotate(f"{row['resistance_rate']:.0f}%",
                                 (row['datetime'], row['resistance_rate']),
                                 xytext=(0, 5), textcoords='offset points', ha='center',
                                 color='red', fontsize=9, fontweight='bold')

            ax2.set_ylabel('耐药率 (%)')
            ax2.set_ylim(-5, 105)
            ax2.legend(loc='upper left')
            ax2.grid(True, linestyle=':', alpha=0.5)

            # X轴日期格式化
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.xticks(rotation=45)

            plt.tight_layout()
            plt.show()


import pandas as pd
import sqlite3
import warnings

# 忽略 pandas 的一些无关紧要的警告
warnings.filterwarnings('ignore')


class DBVisualResistanceMonitor:
    def __init__(self, db_path, src_table="micro_test"):
        """
        初始化：不再接收 DataFrame，而是接收数据库路径
        """
        self.db_path = db_path
        self.src_table = src_table

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _fetch_group_data(self, location, bacteria):
        """
        【关键步骤】只从数据库读取当前需要分析的那一组数据
        即使总数据有1000万条，这里读出来的可能只有 2000 条，内存毫无压力。
        """
        conn = self._get_connection()

        # 1. 读取该组的耐药数据
        # 注意：这里按时间排序，方便后续处理
        sql_res = f"""
        SELECT 
            datetime,
            micro_test_name,
            hospital_location,
            ROUND(
                AVG(
                    CASE 
                        WHEN test_result_other IN ('R', '+') THEN 1.0 
                        ELSE 0.0 
                    END
                ) * 100, 
            2) AS resistance_rate
        FROM {self.src_table}
        WHERE hospital_location = ? AND micro_test_name = ?
        GROUP BY 
            datetime, 
            micro_test_name, 
            hospital_location;
        """
        df_res = pd.read_sql(sql_res, conn, params=(location, bacteria))

        # 2. 读取该组的样本量数据
        sql_cnt = f"""
        SELECT 
            date,  -- 直接使用表中已有的 date 列
            micro_test_name, 
            hospital_location,
            -- 对应 nunique()：统计该日期下有多少个不同的时间戳
            COUNT(DISTINCT time_stamp) AS daily_count
        FROM {self.src_table}
        WHERE hospital_location = ? AND micro_test_name = ?
        GROUP BY 
            date, 
            micro_test_name, 
            hospital_location;
        """
        df_cnt = pd.read_sql(sql_cnt, conn, params=(location, bacteria))

        conn.close()

        return df_res, df_cnt

    def _preprocess_single_group(self, df_res, df_cnt):
        """
        对单组数据进行预处理（逻辑与你之前的一样，但只处理小切片）
        """
        if not df_res.empty:
            df_res['datetime'] = pd.to_datetime(df_res['datetime'])
            df_res['date'] = df_res['datetime'].dt.floor('D')
            # 假设数据库存的是字符串，需要转换；如果是数字则无需这步
            df_res['resistance_rate'] = pd.to_numeric(df_res['resistance_rate'], errors='coerce')

        if not df_cnt.empty:
            df_cnt['date'] = pd.to_datetime(df_cnt['date'])
            df_cnt['daily_count'] = pd.to_numeric(df_cnt['daily_count'], errors='coerce').fillna(0)

        return df_res, df_cnt

    def _analyze_single_group(self, group_res, group_cnt, location, bacteria, window_days, z_threshold):
        """
        核心算法：完全复用你原来的逻辑
        """
        # --- 1. 每日样本量分析 ---
        if group_cnt.empty:
            df_cnt_analysis = pd.DataFrame(columns=['date', 'daily_count', 'pred_count', 'z_cnt', 'is_alert_cnt'])
        else:
            min_date = group_cnt['date'].min()
            # 确保时间轴覆盖所有数据
            max_res_date = group_res['date'].max() if not group_res.empty else min_date
            max_date = max(group_cnt['date'].max(), max_res_date)

            full_idx = pd.date_range(start=min_date, end=max_date, freq='D')

            # 设置索引并重新索引
            ts_cnt = group_cnt.set_index('date')
            # 这里的 drop_duplicates 防止数据库里有重复主键导致 reindex 报错
            ts_cnt = ts_cnt[~ts_cnt.index.duplicated(keep='first')]

            ts_cnt = ts_cnt['daily_count'].reindex(full_idx, fill_value=0).to_frame()

            roll_cnt = ts_cnt['daily_count'].shift(1).rolling(window=window_days, min_periods=1)
            ts_cnt['pred_count'] = roll_cnt.mean()
            std_cnt = roll_cnt.std().replace(0, 1e-6)

            ts_cnt['z_cnt'] = (ts_cnt['daily_count'] - ts_cnt['pred_count']) / std_cnt
            ts_cnt['is_alert_cnt'] = (ts_cnt['z_cnt'] > z_threshold) & (ts_cnt['daily_count'] > 2)

            df_cnt_analysis = ts_cnt.reset_index().rename(columns={'index': 'date'})

        # --- 2. 离散耐药率分析 ---
        if group_res.empty:
            return pd.DataFrame()

        df_res_discrete = group_res.sort_values('datetime').copy()
        df_res_discrete = df_res_discrete.set_index('datetime')

        try:
            indexer = df_res_discrete['resistance_rate'].rolling(window=f'{window_days}D', closed='left', min_periods=1)
            df_res_discrete['pred_res'] = indexer.mean()
            df_res_discrete['std_res'] = indexer.std().replace(0, 1e-6)
        except:
            indexer = df_res_discrete['resistance_rate'].rolling(window=f'{window_days}D', min_periods=1)
            df_res_discrete['pred_res'] = indexer.mean()
            df_res_discrete['std_res'] = indexer.std().replace(0, 1e-6)

        df_res_discrete['z_res'] = (df_res_discrete['resistance_rate'] - df_res_discrete['pred_res']) / df_res_discrete[
            'std_res']
        df_res_discrete['is_alert_res'] = df_res_discrete['z_res'] > z_threshold

        df_res_discrete = df_res_discrete.reset_index()

        # --- 3. 合并 ---
        final_df = pd.merge(df_res_discrete,
                            df_cnt_analysis[['date', 'daily_count', 'pred_count', 'is_alert_cnt']],
                            on='date',
                            how='left')

        final_df['hospital_location'] = location
        final_df['micro_test_name'] = bacteria

        return final_df

    def run_analysis_generator(self,
                               window=7,
                               z_threshold=2.5,
                               start_date=None,
                               end_date=None,
                               target_locations=None,
                               target_bacteria=None,
                               progress_callback=None):
        """
        【重要改变】这是一个生成器 (Generator)。
        它不会一次性返回所有结果，而是算完一个院区的一个细菌，就'吐'出一块结果。
        """
        conn = self._get_connection()

        if isinstance(target_locations, str):
            target_locations = [target_locations]
        if isinstance(target_bacteria, str):
            target_bacteria = [target_bacteria]

        if not target_locations:
            target_locations = None
        if not target_bacteria:
            target_bacteria = None

        # 1. 快速获取所有需要分析的组合 (Distinct)
        # 这步很快，因为 distinct 结果集很小
        print("正在获取待分析列表...")
        query_combos = f"SELECT DISTINCT hospital_location, micro_test_name FROM {self.src_table}"
        combos = pd.read_sql(query_combos, conn)
        conn.close()  # 查完就关，后面循环里再开

        if target_locations:
            combos = combos[combos['hospital_location'].isin(target_locations)]

        if target_bacteria:
            combos = combos[combos['micro_test_name'].isin(target_bacteria)]

            # 如果筛选完没有任务了，直接结束
        if combos.empty:
            print("⚠️ 警告：根据筛选条件，没有找到任何可分析的数据组合。")
            return

        total_tasks = len(combos)
        print(f"✅ 筛选完成，共有 {total_tasks} 个组合需要分析，开始流式处理...")

        s_date = pd.to_datetime(start_date) if start_date else None
        e_date = pd.to_datetime(end_date) if end_date else None

        total_tasks = len(combos)
        print(f"共发现 {total_tasks} 个分析组合，开始流式处理...")

        for i, (_, row) in enumerate(combos.iterrows()):
            current_step = i + 1

            loc = row['hospital_location']
            bact = row['micro_test_name']

            # --- 2. 关键修改：在每次循环开始时调用回调函数 ---
            # 告诉外部：我现在正在处理第 (idx+1) 个任务，共 total_tasks 个
            if progress_callback:
                progress_callback(current_step, total_tasks, f"正在分析: {loc} - {bact}")

            # --- A. 即使你要查2023年的结果，我们也读取全部历史数据 ---
            # 为什么？因为 Rolling Window 需要前7天的数据。
            # 如果只读2023-01-01开始的数据，那么1月1日的基线就是空的，分析就不准了。
            # 读取单一组的历史全量数据（内存占用很小）
            raw_res, raw_cnt = self._fetch_group_data(loc, bact)

            # --- B. 预处理 ---
            df_res, df_cnt = self._preprocess_single_group(raw_res, raw_cnt)

            if df_res.empty:
                continue

            # --- C. 执行全量分析 ---
            analyzed_df = self._analyze_single_group(df_res, df_cnt, loc, bact, window, z_threshold)

            # --- D. 只有在分析计算完成后，才根据用户指定的时间段截取结果 ---
            # 这样保证了每一天的 Z-score 都是基于完整的历史上下文计算的
            if not analyzed_df.empty:
                if s_date:
                    analyzed_df = analyzed_df[analyzed_df['date'] >= s_date]
                if e_date:
                    analyzed_df = analyzed_df[analyzed_df['date'] <= e_date]

                # Yield 结果（而不是 append 到大列表）
                if not analyzed_df.empty:
                    yield analyzed_df
