import sqlite3
import pandas as pd
import streamlit as st
from streamlit_echarts import st_echarts
import math

from matplotlib.ticker import MaxNLocator

@st.cache_data(show_spinner="正在从数据库加载元数据...")
def load_data_from_db(db_path, table_name="micro_test"):
    """
    从数据库加载分析所需的聚合数据和元数据。

    Args:
        db_path: 数据库文件路径
        table_name: 原始数据表名

    Returns:
        df_resistance, df_count, all_locations, all_bacteria, min_date, max_date
    """

    # 检查数据库文件是否存在
    import os
    if not os.path.exists(db_path):
        st.error(f"数据库文件未找到: {db_path}")
        return pd.DataFrame(), pd.DataFrame(), [], [], None, None

    conn = sqlite3.connect(db_path)

    try:
        # ==================================================
        # 1. 获取元数据 (Metadata) - 使用 SQL 极速查询
        # ==================================================

        # 1.1 获取所有院区 (已排序)
        # SQL 的 DISTINCT 比 Pandas 的 unique() 快得多
        sql_loc = f"SELECT DISTINCT hospital_location FROM {table_name} ORDER BY hospital_location"
        all_locations = pd.read_sql(sql_loc, conn)['hospital_location'].tolist()

        # 1.2 获取所有细菌 (已排序)
        sql_bact = f"SELECT DISTINCT micro_test_name FROM {table_name} ORDER BY micro_test_name"
        all_bacteria = pd.read_sql(sql_bact, conn)['micro_test_name'].tolist()

        # 1.3 获取全局时间范围 (Min/Max)
        # 假设时间列名为 'datetime' (如果原表是 '采集时间' 请修改)
        sql_date = f"SELECT MIN(datetime), MAX(datetime) FROM {table_name}"
        date_range = pd.read_sql(sql_date, conn)

        # 转换日期格式
        if not date_range.empty and date_range.iloc[0, 0]:
            min_date = pd.to_datetime(date_range.iloc[0, 0]).date()
            max_date = pd.to_datetime(date_range.iloc[0, 1]).date()
        else:
            min_date, max_date = None, None

        # 核心 SQL：
        # 1. COUNT(*): 统计出现次数
        # 2. WHERE ...: 排除空值
        # 3. GROUP BY: 按细菌名分组
        # 4. ORDER BY ... DESC: 直接在数据库层面排好序
        sql = f"""
                    SELECT 
                        -- 1. 处理日期：相当于 pd.to_datetime().strftime('%Y-%m-%d')
                        STRFTIME('%Y-%m-%d', time_stamp) AS date,
                        
                        -- 2. 分组键
                        micro_test_name,
                        hospital_location,
                        
                        -- 3. 统计唯一值：相当于 ["time_stamp"].nunique()
                        COUNT(DISTINCT time_stamp) AS daily_count
                    
                    FROM {table_name}  -- 替换为你的真实表名
                    
                    -- 4. 分组
                    GROUP BY 
                        STRFTIME('%Y-%m-%d', time_stamp),
                        micro_test_name,
                        hospital_location
                    
                    -- 5. 可选：按时间排序
                    ORDER BY date;
                    """

        df_cnt = pd.read_sql(sql, conn)

        df_cnt['date'] = pd.to_datetime(df_cnt['date'],errors='coerce')

        return df_cnt, all_locations, all_bacteria, min_date, max_date

    except Exception as e:
        st.error(f"读取数据库时发生错误: {e}")
        return None, [], [], None, None

    finally:
        conn.close()

def community_analysis_echarts(
        df,
        time_granularity=7,
        target_bacteria=None,
        target_hospitals=None,
        plot_type="line",
        top_n=10,
        smooth=False,
        height=600
):
    # ========================== 1. 数据清洗 ==========================
    df_clean = df.copy()

    # 1.1 时间归一化 (去除时分秒，确保纯日期)
    df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce').dt.normalize()
    df_clean = df_clean.dropna(subset=['date'])

    # 1.2 筛选
    if target_hospitals:
        df_clean = df_clean[df_clean["hospital_location"].isin(target_hospitals)]
    if target_bacteria:
        df_clean = df_clean[df_clean["micro_test_name"].isin(target_bacteria)]

    df_clean['daily_count'] = pd.to_numeric(df_clean['daily_count'], errors='coerce').fillna(0)

    # ========================== 2. Top-N 逻辑 ==========================
    total_counts = df_clean.groupby("micro_test_name")["daily_count"].sum().sort_values(ascending=False)
    if total_counts.empty:
        st.warning("⚠️ 数据为空，请检查筛选条件")
        return

    time_gran_str = f"{time_granularity}D"
    top_list = total_counts.head(top_n).index.tolist()

    df_clean.loc[~df_clean["micro_test_name"].isin(top_list), "micro_test_name"] = "其他(Others)"
    unique_bacteria = [b for b in top_list]
    if "其他(Others)" in df_clean["micro_test_name"].values:
        unique_bacteria.append("其他(Others)")

    unique_hospitals = sorted(df_clean["hospital_location"].unique())
    n_hospitals = len(unique_hospitals)

    # ========================== 3. 颜色映射 ==========================
    tab20_hex = [
        "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a",
        "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94",
        "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d",
        "#17becf", "#9edae5"
    ]
    colors = (tab20_hex * ((len(top_list) // 20) + 1))[:len(top_list)]
    color_map = dict(zip(top_list, colors))
    color_map["其他(Others)"] = "#d9d9d9"

    # ========================== 4. 数据核心处理 ==========================

    # 4.1 确定全局基准时间
    global_min_date = df_clean['date'].min()
    global_max_date = df_clean['date'].max()

    if pd.isnull(global_min_date):
        st.warning("有效日期数据为空")
        return

    # 4.2 生成标准时间骨架
    full_time_index = pd.date_range(start=global_min_date, end=global_max_date, freq=time_gran_str)
    common_date_strs = full_time_index.strftime('%Y-%m-%d').tolist()

    global_y_max = 0
    processed_data_dict = {}

    # 4.3 循环处理各院区
    for hospital in unique_hospitals:
        h_data = df_clean[df_clean["hospital_location"] == hospital]

        # 即使该院区没数据，稍后也会用 reindex 填 0，这里跳过只是为了省计算
        if h_data.empty:
            continue

        # A. 透视 (按日聚合)
        pivot_df = pd.pivot_table(
            h_data,
            index='date',
            columns='micro_test_name',
            values='daily_count',
            aggfunc='sum'
        ).fillna(0)

        # B. 补全列 (确保细菌种类对齐)
        pivot_df = pivot_df.reindex(columns=unique_bacteria, fill_value=0)

        # C. 重采样
        # origin=global_min_date: 强制从全局最小日期开始切分时间片，确保所有院区的 index 是对齐的
        resampled_df = pivot_df.resample(time_gran_str, origin=global_min_date).sum()

        # D. 补全时间轴 (Reindex)
        # 此时因为 C 步骤对齐了原点，这里的 reindex 才能正确匹配到时间点
        resampled_df = resampled_df.reindex(full_time_index, fill_value=0)

        # E. 平滑 (可选)
        if smooth and plot_type in ["line", "area"]:
            resampled_df = resampled_df.rolling(window=3, min_periods=1, center=True).mean()

        # F. 计算 Max
        current_max = resampled_df.sum(axis=1).max() if plot_type in ["area", "bar"] else resampled_df.max().max()
        if current_max > global_y_max:
            global_y_max = current_max

        processed_data_dict[hospital] = resampled_df

    # Y轴最大值向上取整
    y_axis_limit = math.ceil(global_y_max * 1.05) if global_y_max > 0 else 1

    # ========================== 5. Echarts 渲染 ==========================
    total_width_pct = 92
    gap_pct = 1
    if n_hospitals > 0:
        single_width = (total_width_pct - (gap_pct * (n_hospitals - 1))) / n_hospitals
    else:
        single_width = 90

    option = {
        "animation": False,
        "title": [],
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "shadow" if plot_type == "bar" else "cross"},
            "backgroundColor": "rgba(255, 255, 255, 0.95)",
            "textStyle": {"color": "#333"},
            "confine": True,  # 修复 Tooltip 溢出
        },
        "legend": {
            "data": unique_bacteria,
            "bottom": 0,
            "type": "scroll",
            "padding": [0, 20]
        },
        "grid": [],
        "xAxis": [],
        "yAxis": [],
        "series": [],
        "dataZoom": [
            {
                "type": "slider",
                "show": True,
                "xAxisIndex": list(range(n_hospitals)),
                "bottom": 35,
                "left": "4%",
                "right": "4%"
            },
            {
                "type": "slider",
                "show": True,
                "yAxisIndex": list(range(n_hospitals)),
                "right": "0%",
                "top": "12%",
                "bottom": "18%",
                "width": 20
            }
        ]
    }

    for idx, hospital in enumerate(unique_hospitals):
        resampled_df = processed_data_dict.get(hospital,
                                               pd.DataFrame(0, index=full_time_index, columns=unique_bacteria))

        left_pos = 2 + idx * (single_width + gap_pct)

        option["grid"].append({
            "left": f"{left_pos}%",
            "width": f"{single_width}%",
            "top": "12%",
            "bottom": "18%",
            "containLabel": False
        })

        option["title"].append({
            "text": hospital,
            "left": f"{left_pos + single_width / 2}%",
            "top": "3%",
            "textAlign": "center",
            "textStyle": {"fontSize": 12, "overflow": "truncate", "width": int(single_width * 10)}
        })

        option["xAxis"].append({
            "type": "category",
            "gridIndex": idx,
            "data": common_date_strs,
            "boundaryGap": False if plot_type != "bar" else True,
            "axisLabel": {"show": False},
            "axisTick": {"show": False},
            "axisLine": {"show": True, "lineStyle": {"color": "#ccc"}}
        })

        y_axis_config = {
            "type": "value",
            "gridIndex": idx,
            "min": 0,
            "max": y_axis_limit,
            "splitLine": {"show": True, "lineStyle": {"type": "dashed", "opacity": 0.4}}
        }

        if idx == 0:
            y_axis_config["axisLabel"] = {"show": True, "fontSize": 10}
            y_axis_config["axisLine"] = {"show": False}
        else:
            y_axis_config["axisLabel"] = {"show": False}
            y_axis_config["axisTick"] = {"show": False}
            y_axis_config["axisLine"] = {"show": False}

        option["yAxis"].append(y_axis_config)

        for bac in unique_bacteria:
            data_values = resampled_df[bac].fillna(0).round(2).tolist()

            series_item = {
                "name": bac,
                "type": "line" if plot_type in ["line", "area"] else "bar",
                "xAxisIndex": idx,
                "yAxisIndex": idx,
                "data": data_values,
                "itemStyle": {"color": color_map[bac]},
                "barMaxWidth": 20
            }

            if plot_type == "area":
                series_item["areaStyle"] = {"opacity": 0.6}
                series_item["stack"] = f"total_{idx}"
                series_item["symbol"] = "none"
            elif plot_type == "bar":
                series_item["stack"] = f"total_{idx}"
            elif plot_type == "line":
                series_item["smooth"] = 0.3 if smooth else False
                series_item["symbolSize"] = 3

            option["series"].append(series_item)

    st_echarts(options=option, height=f"{height}px", theme="macarons")


def trend_analysis():
    st.title("📈趋势分析")

    raw_cnt, list_locs, list_bacts, min_d, max_d = load_data_from_db(st.session_state['DB_PATH'], st.session_state['SRC_TABLE'])

    # 初始化多选框默认值
    if 'bacteria_input_key_trend' not in st.session_state:
        st.session_state['bacteria_input_key_trend'] = []

    # ================= UI 配置区域 =================
    with st.container(border=True):
        st.markdown("""
                <div class="config-title">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"></polygon></svg>
                    分析配置
                </div>
                """, unsafe_allow_html=True)

        c1, c2, c3, c4, c5, c6 = st.columns([1, 1, 1, 1, 2, 1])
        options_type = {
            '折线图-趋势变化': 'line',
            '面积图-堆积分布': 'area',
            '柱状图-数量统计': 'bar'
        }
        with c1:
            start_date_input = st.date_input("开始日期", value=min_d, min_value=min_d, max_value=max_d)
        with c2:
            end_date_input = st.date_input("结束日期", value=max_d, min_value=min_d, max_value=max_d)
        with c3:
            time_granularity = st.number_input("时间粒度（天）", value=7, min_value=1)
        with c4:
            chart_type_label = st.selectbox("图表类型", options=options_type.keys())
        with c5:
            bacteria_input = st.multiselect(
                "分析细菌列表",
                options=list_bacts,
                key='bacteria_input_key_trend'
            )
        with c6:
            top_n = st.number_input("TOP N", value=10, min_value=1, key='top_n_key_trend')

        st.write("")
        st.markdown("""
                <div style="margin-bottom: 8px;">
                    <span style="font-size: 12px; font-weight: 600; color: #64748b;">TARGET LOCATIONS</span>
                    <span class="helper-text">(Leaving empty selects all locations)</span>
                </div>
                """, unsafe_allow_html=True)
        try:
            locations_input = st.pills("Locations", options=list_locs, default=[], selection_mode="multi",
                                       label_visibility="collapsed")
        except AttributeError:
            locations_input = st.multiselect("Locations", options=list_locs, default=[])

        st.markdown("<br>", unsafe_allow_html=True)
        chart_type = options_type[chart_type_label]

        c1, c2, c3 = st.columns([5, 1, 1])
        with c2:
            smooth = st.toggle("平滑作图", value=True)
        with c3:
            # 按钮
            run_btn = st.button("生成图表", type="primary", use_container_width=True)


    with st.container(border=True):
        st.write('📊 分析图表')

        # --- 阶段 1: 按钮点击处理 (更新 State) ---
        if run_btn:
            with st.spinner("数据处理中..."):
                # 执行时间筛选
                mask = (raw_cnt['date'] >= pd.to_datetime(start_date_input)) & \
                       (raw_cnt['date'] <= pd.to_datetime(end_date_input))
                filtered_data = raw_cnt.loc[mask]

                # 将所有绘图所需的参数“快照”保存到 session_state
                st.session_state['trend_chart_params'] = {
                    'data': filtered_data,  # 存储筛选后的 DataFrame
                    'granularity': time_granularity,
                    'bacteria': bacteria_input,
                    'hospitals': locations_input,
                    'type': chart_type,
                    'top_n': top_n,
                    'smooth': smooth
                }

        # --- 阶段 2: 绘图渲染 (读取 State) ---
        # 只要 state 里有数据，就进行渲染。
        # 这样即使 run_btn 为 False (用户修改了其他输入框但没点按钮)，图表依然存在。
        if 'trend_chart_params' in st.session_state:
            params = st.session_state['trend_chart_params']

            # 调用绘图函数，传入存储的参数
            community_analysis_echarts(
                df=params['data'],
                time_granularity=params['granularity'],
                target_bacteria=params['bacteria'],
                target_hospitals=params['hospitals'],
                plot_type=params['type'],
                top_n=params['top_n'],
                smooth=params['smooth']
            )
        else:
            st.info("请配置参数并点击“生成图表”按钮。")
