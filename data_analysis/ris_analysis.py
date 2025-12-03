import sqlite3

import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts, JsCode


def process_ris_data(df, target_bacteria_list, time_granularity):
    """
    数据处理：根据传入的细菌列表，清洗 R/I/S/+/ -/SDD 数据并计算时序占比

    :param df: 原始 dataframe
    :param target_bacteria_list: 外部传入的细菌名称列表 (list of strings)
    :param time_granularity: 时间粒度 'W'(周), 'M'(月), 'Q'(季)
    :return: charts_data (字典), valid_bacteria (实际有数据的细菌列表)
    """
    if df is None or df.empty or not target_bacteria_list:
        return {}, []

    # 1. 自动寻找药敏结果列
    res_col = 'test_result_other'

    if res_col not in df.columns:
        st.error(f"未找到药敏结果列，请检查数据列名是否包含: {res_col}")
        return {}, []

    charts_data = {}
    valid_bacteria = []

    # ==================== 核心映射字典 ====================
    ris_mapping = {
        'R': 'R',
        '+': 'R',  # 阳性 -> 耐药
        'I': 'I',
        'SDD': 'I',  # SDD -> 中介
        'S': 'S',
        '-': 'S'  # 阴性 -> 敏感
    }
    # ====================================================

    # 2. 遍历传入的细菌列表
    for bact in target_bacteria_list:
        # 筛选特定细菌
        sub_df = df[df['micro_test_name'] == bact].copy()

        if sub_df.empty:
            continue

        # ==================== 数据清洗逻辑 ====================
        # 转字符 -> 去空格 -> 转大写
        sub_df[res_col] = sub_df[res_col].astype(str).str.strip().str.upper()
        # 应用映射
        sub_df['std_result'] = sub_df[res_col].map(ris_mapping)
        # 过滤无效数据
        sub_df = sub_df.dropna(subset=['std_result'])

        if sub_df.empty:
            continue
        # ====================================================

        # 3. 时间重采样
        # 确保有 date 列
        if 'date' not in sub_df.columns and 'datetime' in sub_df.columns:
            sub_df['date'] = sub_df['datetime']

        sub_df['date'] = pd.to_datetime(sub_df['date'])
        sub_df.set_index('date', inplace=True)

        # 按时间粒度聚合
        granularity = str(time_granularity) + 'D'
        resampled = sub_df.groupby([pd.Grouper(freq=granularity), 'std_result']).size().unstack(fill_value=0)

        # 补全缺失列
        for col in ['R', 'I', 'S']:
            if col not in resampled.columns:
                resampled[col] = 0

        # 确保堆叠顺序
        resampled = resampled[['R', 'I', 'S']]

        # 4. 计算百分比
        totals = resampled.sum(axis=1)
        resampled = resampled[totals > 0]
        totals = totals[totals > 0]

        percent_df = resampled.div(totals, axis=0) * 100
        percent_df.index = percent_df.index.strftime('%Y-%m-%d')

        # 5. 保存有效数据
        charts_data[bact] = {
            "dates": percent_df.index.tolist(),
            "r_pct": percent_df['R'].round(1).tolist(),
            "i_pct": percent_df['I'].round(1).tolist(),
            "s_pct": percent_df['S'].round(1).tolist(),
            "total_count": totals.tolist()
        }
        valid_bacteria.append(bact)

    return charts_data, valid_bacteria


def process_ris_data_from_db(db_path,
                             target_bacteria_list,
                             time_granularity,
                             target_locations=None,
                             start_date=None,
                             end_date=None,
                             table_name="micro_test"):
    """
    数据处理（数据库版）：从数据库读取特定细菌数据，利用 SQL 完成 RIS 清洗，
    然后在 Pandas 中进行时序聚合计算。

    :param db_path: 数据库文件路径
    :param target_bacteria_list: 外部传入的细菌名称列表 (list of strings)
    :param time_granularity: 时间粒度天数
    :param target_locations: 分析院区列表
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param table_name: 表名，默认为 micro_test
    :return: charts_data (字典), valid_bacteria (实际有数据的细菌列表)
    """
    if not target_bacteria_list:
        return {}, []

    # 1. 建立数据库连接
    conn = sqlite3.connect(db_path)

    try:
        # ==================== 核心优化：将映射逻辑移至 SQL ====================
        # 使用 CASE WHEN 在数据库层面完成 R/I/S 映射和清洗
        # 这样读入内存的就已经是干净的 'R', 'I', 'S' 或 NULL，极大减少内存占用

        # 构造 SQL 的 IN 查询占位符 (?, ?, ?)
        placeholders = ','.join(['?'] * len(target_bacteria_list))

        query_params = list(target_bacteria_list)

        sql = f"""
        SELECT 
            datetime,
            micro_test_name,
            CASE 
                WHEN UPPER(TRIM(test_result_other)) IN ('R', '+') THEN 'R'
                WHEN UPPER(TRIM(test_result_other)) IN ('I', 'SDD') THEN 'I'
                WHEN UPPER(TRIM(test_result_other)) IN ('S', '-') THEN 'S'
                ELSE NULL 
            END AS std_result
        FROM {table_name}
        WHERE micro_test_name IN ({placeholders})
          AND std_result IS NOT NULL -- 直接在数据库层过滤无效数据
        """

        # 2. 动态追加：院区筛选 (Hospital Location)
        if target_locations and len(target_locations) > 0:
            # 生成对应数量的 ? 占位符
            loc_placeholders = ','.join(['?'] * len(target_locations))

            # 追加 SQL
            sql += f" AND hospital_location IN ({loc_placeholders})"

            # 追加参数 (注意顺序要和 SQL 中的占位符一致)
            query_params.extend(target_locations)

        if start_date:
            sql += " AND datetime >= ?"
            # 确保转为字符串比较 (SQLite 兼容性)
            query_params.append(str(start_date))

        if end_date:
            sql += " AND datetime <= ?"
            # 【关键】如果 end_date 只是日期 '2023-01-01'，
            # 数据库里的 '2023-01-01 10:00:00' 会比它大，从而被过滤掉。
            # 所以需要补全时间到当天的最后一秒，或者由调用方保证传入的是 datetime
            e_date_str = str(end_date)
            if len(e_date_str) == 10:  # 如果是 'YYYY-MM-DD' 格式
                e_date_str += " 23:59:59.999"
            query_params.append(e_date_str)

        # 2. 读取数据 (一次性读取所有目标细菌，减少 IO 次数)
        # params=target_bacteria_list 会自动填充上面的 ? 占位符
        df_raw = pd.read_sql(sql, conn, params=query_params)

    except Exception as e:
        st.error(f"数据库查询失败: {e}")
        return {}, []
    finally:
        conn.close()

    if df_raw.empty:
        return {}, []

    # 3. 数据预处理
    # 确保时间列为 datetime 类型
    if 'datetime' in df_raw.columns:
        df_raw['date'] = pd.to_datetime(df_raw['datetime'])
    else:
        st.error("数据库中未找到 datetime 列")
        return {}, []

    charts_data = {}
    valid_bacteria = []

    # 4. 按细菌分组处理 (此时数据已在内存，操作 Pandas 很快)
    # 使用 groupby 避免多次 query dataframe
    for bact, sub_df in df_raw.groupby('micro_test_name'):

        # 设置时间索引用于重采样
        sub_df = sub_df.set_index('date').sort_index()

        # ==================== 时间聚合逻辑 ====================
        # 处理时间粒度：如果传入的是数字(天数)，加'D'；如果是 W/M/Q，直接用
        freq_str = str(time_granularity)
        if freq_str.isdigit():
            freq_str += 'D'

        try:
            # 按时间粒度 + 结果类型聚合计数
            # unstack(fill_value=0) 将 R/I/S 转为列
            resampled = sub_df.groupby([pd.Grouper(freq=freq_str), 'std_result']).size().unstack(fill_value=0)
        except Exception as e:
            # 防止无效的 freq 报错
            st.warning(f"时间聚合失败 ({bact}): {e}")
            continue

        # 5. 补全缺失列 (防止某段时间只有 S 没有 R)
        for col in ['R', 'I', 'S']:
            if col not in resampled.columns:
                resampled[col] = 0

        # 确保列顺序一致
        resampled = resampled[['R', 'I', 'S']]

        # 6. 计算百分比
        totals = resampled.sum(axis=1)

        # 过滤掉总数为0的时间点
        valid_indices = totals > 0
        resampled = resampled[valid_indices]
        totals = totals[valid_indices]

        if resampled.empty:
            continue

        percent_df = resampled.div(totals, axis=0) * 100

        # 格式化日期索引为字符串
        percent_df.index = percent_df.index.strftime('%Y-%m-%d')

        # 7. 组装结果
        charts_data[bact] = {
            "dates": percent_df.index.tolist(),
            "r_pct": percent_df['R'].round(1).tolist(),
            "i_pct": percent_df['I'].round(1).tolist(),
            "s_pct": percent_df['S'].round(1).tolist(),
            "total_count": totals.tolist()
        }
        valid_bacteria.append(bact)

    return charts_data, valid_bacteria

def plot_ris_trend_echarts(charts_data, bact_name):
    """
    绘制单个细菌的 100% 堆叠柱状图 (字符串模板修复版)
    """
    data = charts_data.get(bact_name)
    if not data: return

    option = {
        "title": {
            "text": bact_name,
            "left": "center",
            "textStyle": {"fontSize": 14}
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "shadow"},
            "backgroundColor": "rgba(255, 255, 255, 0.9)",
            "textStyle": {"color": "#333"},

            # 🌟 核心修改：使用字符串模板替代 JsCode
            # {b}  代表类目轴的值（日期）
            # {a0} 代表第1个系列的名称(耐药R)，{c0} 代表其数值
            # {a1} 代表第2个系列的名称(中介I)，{c1} 代表其数值
            # {a2} 代表第3个系列的名称(敏感S)，{c2} 代表其数值
            # 直接在后面手动加上 % 符号
            "formatter": "{b}<br />{a0}: {c0}%<br />{a1}: {c1}%<br />{a2}: {c2}%"
        },
        "legend": {
            "data": ["耐药(R)", "中介(I)", "敏感(S)"],
            "top": "25px",
            "itemWidth": 10,
            "itemHeight": 10
        },
        "grid": {
            "left": "3%", "right": "4%", "bottom": "3%", "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": data['dates'],
            "axisLabel": {"rotate": 45, "fontSize": 10}
        },
        "yAxis": {
            "type": "value",
            "min": 0, "max": 100,
            "axisLabel": {"formatter": "{value}%"}
        },
        "series": [
            {
                "name": "耐药(R)",
                "type": "bar",
                "stack": "total",
                "data": data['r_pct'],
                "itemStyle": {"color": "#ff4d4f"},
                "barWidth": "60%"
            },
            {
                "name": "中介(I)",
                "type": "bar",
                "stack": "total",
                "data": data['i_pct'],
                "itemStyle": {"color": "#fadb14"},
                "barWidth": "60%"
            },
            {
                "name": "敏感(S)",
                "type": "bar",
                "stack": "total",
                "data": data['s_pct'],
                "itemStyle": {"color": "#52c41a"},
                "barWidth": "60%"
            }
        ]
    }

    st_echarts(option, height="300px", key=f"ris_{bact_name}")
