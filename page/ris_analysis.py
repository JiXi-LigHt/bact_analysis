import sqlite3
import pandas as pd
from data_analysis.ris_analysis import plot_ris_trend_echarts, process_ris_data_from_db
import streamlit as st

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

def ris_analysis_page():

    st.title("🦠 重点耐药菌 R/I/S 时序构成分析")

    raw_cnt, list_locs, list_bacts, min_d, max_d = load_data_from_db(st.session_state['DB_PATH'], st.session_state['SRC_TABLE'])

    def on_top_n_change():
        """当 Top N 输入框变化时执行此函数"""
        # 获取当前的 top_n 值
        n = st.session_state.get('top_n_key_dashboard', 0)

        if n > 0:
            # 计算 Top N 细菌 (传入你的原始数据 df)
            # 注意：这里需要确保 df 在此作用域可见，或者存放在 st.session_state['analysis_results'] 中

            if raw_cnt is not None and not raw_cnt.empty:
                # 获取按 count 排序的前n个细菌列表
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

        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 2, 1])
        with c1:
            start_date_input = st.date_input("开始日期", value=min_d, min_value=min_d, max_value=max_d)
        with c2:
            end_date_input = st.date_input("结束日期", value=max_d, min_value=min_d, max_value=max_d)
        with c3:
            granularity = st.number_input("时间粒度（天）", value=7, min_value=0)
        with c4:
            bacteria_input = st.multiselect(
                "分析细菌列表",
                options=list_bacts,
                key='bacteria_input_key_dashboard',
                on_change=on_bact_change,
            )

        with c5:
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

    if run_btn:
        st.session_state['analysis_snapshot'] = {
            'bacteria_list': st.session_state['bacteria_input_key_dashboard'],
            'granularity': granularity,
            'locations': locations_input,
            'start_date': start_date_input,
            'end_date': end_date_input,
            'db_path': st.session_state.get('DB_PATH'),
            'table_name': st.session_state.get('SRC_TABLE')
        }
        st.session_state['need_fetch_data'] = True

        # 检查是否有快照数据（即是否至少点击过一次运行）
    if 'analysis_snapshot' not in st.session_state:
        # 如果还没运行过，直接返回，什么都不显示
        return

    # 获取快照中的配置（注意：这里不再直接使用 input 组件的变量，而是用 snapshot 里的）
    config = st.session_state['analysis_snapshot']

    if st.session_state.get('need_fetch_data', False) or 'cached_charts_data' not in st.session_state:

        # 初始化结果容器
        final_charts_data = {}
        final_valid_list = []

        # 初始化进度条容器
        progress_container = st.empty()
        with progress_container.container():
            status_text = st.empty()
            progress_bar = st.progress(0)
        target_bacts = st.session_state['bacteria_input_key_dashboard']
        total_tasks = len(target_bacts)

        # --- 循环获取数据 (Loop) ---
        for idx, bact in enumerate(target_bacts):
            # 1. 更新进度条
            percent = min((idx + 1) / total_tasks, 1.0)
            progress_bar.progress(percent)
            status_text.text(f"[{idx + 1}/{total_tasks}] 正在分析: {bact}...")

            # 2. 查询单个细菌的数据 (这里是耗时操作)
            # 注意：target_bacteria_list 传入的是单元素列表 [bact]
            single_chart_data, single_valid = process_ris_data_from_db(
                db_path=config['db_path'],
                target_bacteria_list=[bact],
                time_granularity=config['granularity'],
                target_locations=config['locations'],
                start_date=config['start_date'],
                end_date=config['end_date'],
                table_name=config['table_name']
            )

            # 3. 合并结果
            if single_chart_data:
                final_charts_data.update(single_chart_data)
            if single_valid:
                final_valid_list.extend(single_valid)

        # 数据获取完成，存入缓存
        st.session_state['cached_charts_data'] = final_charts_data
        st.session_state['cached_valid_list'] = final_valid_list
        st.session_state['need_fetch_data'] = False  # 重置标记，下次非按钮刷新时直接读缓存

        # 清除进度条
        progress_container.empty()

    charts_data = st.session_state['cached_charts_data']
    top_bacteria_list = st.session_state['cached_valid_list']
    # 校验数据
    if not top_bacteria_list:
        st.warning("⚠️ 未找到有效数据，请调整分析配置（如日期范围或细菌列表）。")
        del st.session_state['analysis_snapshot']
        return

    # 图表渲染 (这里使用进度条)
    with st.container(border=True):
        cols_per_row = 2
        rows = [st.container() for _ in range((len(top_bacteria_list) + 1) // cols_per_row)]
        total_tasks = len(top_bacteria_list)

        if run_btn:
            # === 动画模式 ===
            progress_container = st.empty()
            with progress_container.container():
                status_text = st.empty()
                progress_bar = st.progress(0)

            for idx, bact in enumerate(top_bacteria_list):
                # 更新进度
                percent = min((idx + 1) / total_tasks, 1.0)
                progress_bar.progress(percent)
                status_text.text(f"[{idx + 1}/{total_tasks}] 正在绘制: {bact}")

                # 绘图逻辑
                row_idx = idx // cols_per_row
                col_idx = idx % cols_per_row
                with rows[row_idx]:
                    if col_idx == 0:
                        cols = st.columns(cols_per_row)
                    with cols[col_idx]:
                        plot_ris_trend_echarts(charts_data, bact)
                        total_samples = sum(charts_data[bact]['total_count'])
                        st.caption(f"总样本量: {total_samples}")

            progress_container.empty()  # 动画结束后清除进度条

        else:
            # === 静态模式 (直接展示，无进度条，体验更好) ===
            # 当你修改了配置但没点运行，或者仅仅是缩放了浏览器窗口时，走这里
            for idx, bact in enumerate(top_bacteria_list):
                row_idx = idx // cols_per_row
                col_idx = idx % cols_per_row
                with rows[row_idx]:
                    if col_idx == 0:
                        cols = st.columns(cols_per_row)
                    with cols[col_idx]:
                        plot_ris_trend_echarts(charts_data, bact)
                        total_samples = sum(charts_data[bact]['total_count'])
                        st.caption(f"总样本量: {total_samples}")