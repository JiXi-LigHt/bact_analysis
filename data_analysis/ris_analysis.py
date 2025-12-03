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
