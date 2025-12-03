import sqlite3
import pandas as pd
import streamlit as st
from streamlit_echarts import st_echarts
from data_analysis.anomaly_detect import DBVisualResistanceMonitor

def render_kpi(col, title, value, sub_text, icon_html, is_alert=False):
    color_class = "color: #d63031;" if is_alert else "color: #333;"
    bg_icon = "#ffe5e5" if is_alert else "#f8f9fa"
    icon_color = "#d63031" if is_alert else "#666"

    html = f"""
    <div class="card-container">
        <div style="display: flex; justify-content: space-between; align-items: start;">
            <div>
                <div class="kpi-title">{title}</div>
                <div class="kpi-value" style="{color_class}">{value}</div>
            </div>
            <div style="background: {bg_icon}; padding: 10px; border-radius: 8px; color: {icon_color}; font-size: 20px;">
                {icon_html}
            </div>
        </div>
        <div class="kpi-sub">{sub_text}</div>
    </div>
    """
    col.markdown(html, unsafe_allow_html=True)


def plot_anomalies_echarts(df_result, bact_name, loc_name):
    """
    可视化功能：使用 ECharts 绘制交互式异常监测图
    """
    # 1. 数据检查与清洗
    if df_result is None or df_result.empty:
        st.warning("当前日期范围内无数据或无异常。")
        return

    # 筛选并排序
    data = df_result[(df_result['hospital_location'] == loc_name) &
                     (df_result['micro_test_name'] == bact_name)].sort_values('datetime')

    if data.empty:
        return

    # ========================== 数据预处理 ==========================

    # 清洗函数：将 NaN 转为 None
    def clean_nan(val):
        if pd.isna(val): return None
        return val

    # --- 准备 Plot 1 数据 (每日统计) ---
    daily_data = data[['date', 'daily_count', 'pred_count', 'is_alert_cnt']].drop_duplicates('date').sort_values('date')

    # 1. 生成 X 轴的类目列表（日期字符串）
    daily_dates = daily_data['date'].astype(str).tolist()

    # 2. 清洗 Y 轴数值
    daily_counts = [clean_nan(x) for x in daily_data['daily_count']]
    pred_counts = [clean_nan(x) for x in daily_data['pred_count']]

    # 3. 提取预警点，使用 Index (0, 1, 2...) 作为 X 坐标
    # 这样避免了日期字符串格式不一致导致 ECharts 无法匹配的问题
    alert_cnt_data = []
    # 使用 reset_index 确保我们可以安全地遍历
    daily_data_reset = daily_data.reset_index(drop=True)

    for idx, row in daily_data_reset.iterrows():
        if row['is_alert_cnt']:  # 如果是异常点
            val = clean_nan(row['daily_count'])
            if val is not None:
                # 格式：[X轴索引, Y轴数值]
                alert_cnt_data.append([idx, val])

    # --- 准备 Plot 2 数据 (耐药率详情) ---
    datetime_strs = data['datetime'].dt.strftime('%Y-%m-%d %H:%M').tolist()

    # 清洗耐药率数据
    pred_res_vals = [clean_nan(x) for x in data['pred_res']]
    line_res_data = list(zip(datetime_strs, pred_res_vals))

    # 正常点
    normal_points = data[~data['is_alert_res']]
    norm_dates = normal_points['datetime'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    norm_vals = [clean_nan(x) for x in normal_points['resistance_rate']]
    scatter_normal_data = list(zip(norm_dates, norm_vals))

    # 异常点
    alert_points = data[data['is_alert_res']]
    alert_dates = alert_points['datetime'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    alert_vals = [clean_nan(x) for x in alert_points['resistance_rate']]
    scatter_alert_data = list(zip(alert_dates, alert_vals))

    date_min_str = data['date'].min().strftime('%Y-%m-%d')
    date_max_str = data['date'].max().strftime('%Y-%m-%d')

    # ========================== 配置 ECharts Option ==========================
    option = {
        "title": {
            "text": f"异常监测: {loc_name} - {bact_name}",
            "subtext": f"({date_min_str} 至 {date_max_str})",
            "left": "center",
            "textStyle": {"fontSize": 16, "fontWeight": "bold"}
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross"},
            "backgroundColor": "rgba(255, 255, 255, 0.9)"
        },
        "legend": {
            "top": "10%",
            "data": ["每日检出数", "基线", "爆发预警", "预测基线", "正常检测", "耐药异常"]
        },
        "grid": [
            {"left": "5%", "right": "5%", "top": "18%", "height": "25%"},
            {"left": "5%", "right": "5%", "top": "50%", "height": "40%"}
        ],
        "xAxis": [
            {
                "type": "category",
                "data": daily_dates,
                "gridIndex": 0,
                "axisLabel": {"show": False},
                "axisTick": {"alignWithLabel": True}
            },
            {
                "type": "time",
                "gridIndex": 1,
                "axisLabel": {"formatter": "{MM}-{dd}"},
                "splitLine": {"show": True, "lineStyle": {"type": "dashed", "opacity": 0.3}}
            }
        ],
        "yAxis": [
            {
                "type": "value",
                "name": "样本量",
                "gridIndex": 0,
                "splitLine": {"show": True, "lineStyle": {"type": "dashed", "opacity": 0.3}}
            },
            {
                "type": "value",
                "name": "耐药率 (%)",
                "gridIndex": 1,
                "min": -5, "max": 105,
                "splitLine": {"show": True, "lineStyle": {"type": "dashed", "opacity": 0.5}}
            }
        ],
        "dataZoom": [
            {
                "type": "slider",
                "xAxisIndex": [0, 1],
                "bottom": "2%"
            },
            {
                "type": "inside",
                "xAxisIndex": [0, 1]
            }
        ],
        "series": [
            # --- Plot 1: 样本量 ---
            {
                "name": "每日检出数",
                "type": "bar",
                "xAxisIndex": 0, "yAxisIndex": 0,
                "data": daily_counts,
                "itemStyle": {"color": "#e0e0e0"},
                "barWidth": "60%"
            },
            {
                "name": "基线",
                "type": "line",
                "xAxisIndex": 0, "yAxisIndex": 0,
                "data": pred_counts,
                "itemStyle": {"color": "orange"},
                "lineStyle": {"type": "dashed"},
                "symbol": "none"
            },
            {
                "name": "爆发预警",
                "type": "scatter",
                "xAxisIndex": 0, "yAxisIndex": 0,
                "data": alert_cnt_data,  # 现在是 [[0, 3], [5, 4]...] 这种格式

                # 使用倒三角，并将其悬浮在柱子上方
                "symbol": "triangle",
                "symbolRotate": 180,  # 旋转180度变成倒三角
                "symbolOffset": [0, '-50%'],  # 向上偏移，防止被柱子遮挡
                "symbolSize": 15,  # 稍微大一点更醒目
                "itemStyle": {"color": "red"},
                "z": 10  # 确保图层在最上层
            },

            # --- Plot 2: 耐药率 ---
            {
                "name": "预测基线",
                "type": "line",
                "xAxisIndex": 1, "yAxisIndex": 1,
                "data": line_res_data,
                "showSymbol": False,
                "lineStyle": {"color": "green", "width": 1.5, "opacity": 0.6},
                "smooth": True,
                "connectNulls": False
            },
            {
                "name": "正常检测",
                "type": "scatter",
                "xAxisIndex": 1, "yAxisIndex": 1,
                "data": scatter_normal_data,
                "itemStyle": {"color": "gray", "opacity": 0.5},
                "symbolSize": 6
            },
            {
                "name": "耐药异常",
                "type": "scatter",
                "xAxisIndex": 1, "yAxisIndex": 1,
                "data": scatter_alert_data,
                "itemStyle": {"color": "red", "borderColor": "black", "borderWidth": 1},
                "symbolSize": 12,
                "z": 10,
                "label": {
                    "show": True,
                    "formatter": "{@1}%",
                    "position": "top",
                    "color": "red",
                    "fontWeight": "bold"
                }
            }
        ],
        "animationDuration": 1000,
        "animationDurationUpdate": 1000,
        "animationEasing": "cubicOut",
        "animationEasingUpdate": "quinticInOut"
    }

    st_echarts(options=option, height="600px", key=f"echarts_{loc_name}_{bact_name}")


def render_custom_card(row, full_history, loc):
    """
    渲染单个交互式卡片：HTML信息 + 分析按钮 + 折叠图表
    """
    bact = row['micro_test_name']
    date_str = row['datetime'].strftime('%Y-%m-%d')

    # 生成唯一 Key
    card_key = f"card_{loc}_{bact}_{date_str}"

    # 使用 st.container(border=True) 模拟卡片外观
    with st.container(border=True):

        # 布局：左侧信息 (HTML)，右侧按钮
        c1, c2 = st.columns([0.75, 0.25])

        with c1:
            # 构造 HTML 标签
            tags_html = ""
            if row['is_alert_res']:
                tags_html += f'<span class="tag-pill tag-res">📉 耐药: {row["resistance_rate"]:.1f}%</span>'
            if row['is_alert_cnt']:
                tags_html += f'<span class="tag-pill tag-cnt">👥 激增: {int(row["daily_count"])}例</span>'

            html_content = f"""
            <div style="line-height: 1.4;">
                <div class="card-title-row">
                    <span class="bact-name">🦠 {bact}</span>
                    <span class="alert-date">📅 {date_str}</span>
                </div>
                <div class="tag-row">{tags_html}</div>
            </div>
            """
            st.markdown(html_content, unsafe_allow_html=True)

        with c2:
            # 交互按钮
            is_expanded = st.session_state.get(card_key, False)
            btn_label = "📉 分析" if not is_expanded else "❌ 收起"

            if st.button(btn_label, key=f"btn_{card_key}", width='stretch'):
                st.session_state[card_key] = not is_expanded
                st.rerun()

        # 展开图表区域
        if st.session_state.get(card_key, False):
            st.markdown("---")
            # 过滤该细菌在该院区的全量历史数据
            history_data = full_history[
                (full_history['hospital_location'] == loc) &
                (full_history['micro_test_name'] == bact)
                ]

            if not history_data.empty:
                plot_anomalies_echarts(history_data, bact, loc)
            else:
                st.caption("暂无历史数据")


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
                        micro_test_name, 
                        COUNT(*) as total_count
                    FROM {table_name}
                    WHERE micro_test_name IS NOT NULL AND micro_test_name != ''
                    GROUP BY micro_test_name
                    ORDER BY total_count DESC
                    """

        df_cnt = pd.read_sql(sql, conn)

        return df_cnt, all_locations, all_bacteria, min_date, max_date

    except Exception as e:
        st.error(f"读取数据库时发生错误: {e}")
        return None, [], [], None, None

    finally:
        conn.close()


def dashboard():
    st.title("🖥️信息面板及异常检测")
    # 加载原始数据
    raw_cnt, list_locs, list_bacts, min_d, max_d = load_data_from_db(st.session_state['DB_PATH'], st.session_state['SRC_TABLE'])
    st.markdown("""
    <style>

        /* === KPI 卡片样式 === */
        .card-container {
            background-color: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            border: 1px solid #f0f2f6;
            margin-bottom: 20px;
        }
        .kpi-title { font-size: 14px; color: #666; margin-bottom: 5px; }
        .kpi-value { font-size: 32px; font-weight: bold; color: #333; }
        .kpi-sub { font-size: 12px; color: #ff4b4b; margin-top: 5px; }

        /* === 院区标题头样式 (新版) === */
        .loc-header-box {
            background-color: #f8f9fa;
            border: 1px solid #e0e0e0;
            border-bottom: 3px solid #eee; /* 底部加粗分隔 */
            border-radius: 8px 8px 0 0;
            padding: 12px 15px;
            display: flex; justify-content: space-between; align-items: center;
            margin-bottom: 0px; /* 紧贴下方的滚动区 */
            margin-top: 10px;
        }
        .loc-title { font-size: 16px; font-weight: 700; color: #333; }
        .loc-badge { background: #ffe5e5; color: #d63031; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: bold; }

        /* === 卡片内部文字样式 === */
        .card-title-row { display: flex; align-items: center; margin-bottom: 6px; }
        .bact-name { font-size: 15px; font-weight: 700; color: #2c3e50; margin-right: 10px; }
        .alert-date { font-size: 12px; color: #95a5a6; background-color: #f4f6f7; padding: 2px 6px; border-radius: 4px; }

        /* === 标签样式 (Pills) === */
        .tag-row { display: flex; gap: 8px; }
        .tag-pill { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
        .tag-res { background-color: #fff1f0; color: #cf1322; border: 1px solid #ffa39e; }
        .tag-cnt { background-color: #fff7e6; color: #d46b08; border: 1px solid #ffd591; }

        /* === 调整 Streamlit 原生按钮样式 === */
        div[data-testid="stVerticalBlock"] div[data-testid="stButton"] { text-align: right; }
        button[kind="secondary"] { border-radius: 6px; font-size: 12px; height: auto; padding: 4px 10px; }

        /* === 滚动容器微调 === */
        div[data-testid="stVerticalBlockBorderWrapper"] {
            margin-bottom: 8px;
            background-color: white;
            transition: box-shadow 0.2s;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-color: #d9d9d9;
        }

        /* 右侧 Chart 容器 */
        .chart-box { border-left: 1px solid #eee; padding-left: 20px; }
        
    
    </style>
    """, unsafe_allow_html=True)

    def on_top_n_change():
        """当 Top N 输入框变化时执行此函数"""
        # 获取当前的 top_n 值
        n = st.session_state.get('top_n_key_dashboard', 0)

        if n > 0:
            # 计算 Top N 细菌 (传入你的原始数据 df)
            # 注意：这里需要确保 df 在此作用域可见，或者存放在 st.session_state['analysis_results'] 中

            if raw_cnt is not None and not raw_cnt.empty:
                # 调用之前的函数获取列表
                # top_list = get_top_n_bacteria(raw_cnt, n)

                top_list = raw_cnt.head(n)['micro_test_name'].tolist()

                # 过滤：确保计算出的细菌确实在下拉选项 list_bacts 中，防止报错
                valid_top_list = [b for b in top_list if b in list_bacts]

                # 更新多选框的状态
                st.session_state['bacteria_input_key_dashboard'] = valid_top_list

    def on_bact_change():
        """当 bact 输入框变化时执行此函数"""
        # 获取当前的 top_n 值
        st.session_state['top_n_key_dashboard'] = 0

    # 初始化多选框的默认值 (如果 session_state 中没有)
    if 'bacteria_input_key_dashboard' not in st.session_state:
        # 默认为空
        st.session_state['bacteria_input_key_dashboard'] = []

    if "top_n_key_dashboard" not in st.session_state:
        st.session_state['top_n_key_dashboard'] = 0

    with st.container(border=True):
        st.markdown("""
                <div class="config-title">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"></polygon></svg>
                    分析配置
                </div>
                """, unsafe_allow_html=True)

        c1, c2, c3, c4, c5, c6 = st.columns([1, 1, 1, 1, 2, 1])
        with c1:
            start_date_input = st.date_input("开始日期", value=min_d, min_value=min_d, max_value=max_d)
        with c2:
            end_date_input = st.date_input("结束日期", value=max_d, min_value=min_d, max_value=max_d)
        with c3:
            window_input = st.number_input("滑动窗口", value=7, min_value=3)
        with c4:
            z_input = st.number_input("Z-SCORE", value=2.5, min_value=1.2)
        with c5:
            bacteria_input = st.multiselect(
                "分析细菌列表",
                options=list_bacts,
                key='bacteria_input_key_dashboard',
                on_change=on_bact_change,
            )

        with c6:
            top_n = st.number_input(
                "TOP N",
                min_value=0,
                key='top_n_key_dashboard',
                on_change=on_top_n_change
            )

        st.write("")
        st.markdown("""
                <div style="margin-bottom: 8px;">
                    <span style="font-size: 12px; font-weight: 600; color: #64748b;">分析院区选择</span>
                    <span class="helper-text">(默认选择全部院区)</span>
                </div>
                """, unsafe_allow_html=True)
        try:
            locations_input = st.pills("Locations", options=list_locs, default=[], selection_mode="multi",
                                       label_visibility="collapsed")

        except AttributeError:
            locations_input = st.multiselect("Locations", options=list_locs, default=[])

        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2 = st.columns([5, 1])
        with c2:
            run_btn = st.button("生成图表", type="primary", use_container_width=True)

    if 'analysis_results' not in st.session_state or run_btn:
        progress_container = st.empty()

        # 在容器内部初始化组件
        with progress_container.container():
            status_text = st.empty()  # 文本显示在上方
            progress_bar = st.progress(0)  # 进度条显示在下方

        # 定义回调函数 (连接后端逻辑与前端 UI 的桥梁)
        def update_progress(current, total, message):
            # 计算百分比 (0.0 到 1.0)
            percent = current / total
            # 更新 Streamlit 组件
            progress_bar.progress(percent)
            status_text.text(f"[{current}/{total}] {message}")

        results_buffer = []
        db_monitor = DBVisualResistanceMonitor(st.session_state['DB_PATH'], st.session_state['SRC_TABLE'])
        generator = db_monitor.run_analysis_generator(
            window=window_input,
            z_threshold=z_input,
            start_date=start_date_input,
            end_date=end_date_input,
            target_locations=locations_input,
            target_bacteria=bacteria_input,
            progress_callback=update_progress,
        )
        for df_chunk in generator:
            results_buffer.append(df_chunk)

        progress_container.empty()

        if results_buffer:
            new_df_result = pd.concat(results_buffer, ignore_index=True)
            st.session_state['analysis_results'] = new_df_result

    # 1. 安全读取数据
    df_result = st.session_state.get('analysis_results')

    # 2. 初始化默认值
    total_records = 0
    active_alerts = 0
    total_locs = 0
    affected_locs = 0
    unique_locations = []
    latest_alerts = pd.DataFrame()

    # 3. 如果有数据，进行计算覆盖默认值
    if df_result is not None and not df_result.empty:
        # 数据存在，开始计算 KPI
        alerts_df = df_result[df_result['is_alert_cnt'] | df_result['is_alert_res']].copy()

        # 这里的排序逻辑建议放在这里，因为 alerts_df 是临时的
        alerts_df = alerts_df.sort_values('date', ascending=False)

        latest_alerts = alerts_df.drop_duplicates(['hospital_location', 'micro_test_name'])

        # 计算 KPI
        total_records = len(df_result)
        active_alerts = len(latest_alerts)
        total_locs = df_result['hospital_location'].nunique()
        affected_locs = alerts_df['hospital_location'].nunique()
        unique_locations = alerts_df['hospital_location'].unique()

    elif df_result is None or df_result.empty:
        # 显式处理空数据情况（可选，因为上面已经初始化了默认值）
        # 如果是点击了 Run 依然为空，可以在这里显示警告
        if run_btn:
            st.warning("⚠️ 当前筛选条件下未查询到数据 (No data found).")

    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        render_kpi(kpi1, "总记录数", f"{total_records}", "Total Records", "📈")
    with kpi2:
        render_kpi(kpi2, "异常报警数", f"{active_alerts}", "Based on Rate & Count Flags", "⚠️", is_alert=True)
    with kpi3:
        render_kpi(kpi3, "影响院区", f"{affected_locs} / {total_locs}", "Campus Spread", "🏥")

    # ==========================================
    # 界面布局：主内容区
    # ==========================================
    st.markdown("### 🚫 异常检测详情")

    # 两列布局
    c1, c2 = st.columns(2)
    cols = [c1, c2]

    for idx, loc in enumerate(unique_locations):
        target_col = cols[idx % 2]

        # 获取该院区下的预警摘要列表
        loc_data = latest_alerts[latest_alerts['hospital_location'] == loc]
        alert_count = len(loc_data)

        with target_col:
            # A. 院区标题头 (HTML)
            st.markdown(f"""
            <div class="loc-header-box">
                <span class="loc-title">📍 {loc}</span>
                <span class="loc-badge">{alert_count} Alerts</span>
            </div>
            """, unsafe_allow_html=True)

            # B. 滚动区域容器 (使用 st.container 实现固定高度)
            with st.container(height=800):
                if loc_data.empty:
                    st.caption("No alerts")
                else:
                    for _, row in loc_data.iterrows():
                        # 调用自定义卡片渲染函数
                        # 传入：当前异常行，全量历史数据(df_result)，当前院区名
                        render_custom_card(row, df_result, loc)
