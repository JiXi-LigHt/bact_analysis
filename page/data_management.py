import numpy as np
import pandas as pd
import streamlit as st
import time

from data_process.data_generate import generate_micro_demo_data
from data_process.data_processer import extract_hospital_location


def clean_data(df):
    """
    标准化清洗数据，消除格式差异导致的去重失败
    """
    df = df.copy()

    # 1. 统一处理空值：将所有形式的空值统一替换为 None 或 np.nan
    # 这里我们把空字符串、只有空格的字符串都变为空值
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # 2. 统一处理字符串：去除首尾空格
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    # 3. 强制转换关键列的类型
    # 假设 '采集时间' 是去重关键，必须统一格式
    if '采集时间' in df.columns:
        df['采集时间'] = pd.to_datetime(df['采集时间'], errors='coerce')
        # 【关键】如果不需要精确到秒，可以舍弃秒之后的时间，大幅提高去重率
        # df['采集时间'] = df['采集时间'].dt.floor('Min')  # 强制舍弃秒，精确到分


    return df

def data_management():
    st.title('数据管理')

    # 确保 session_state 初始化
    if 'main_data' not in st.session_state:
        st.session_state['main_data'] = pd.DataFrame()

    # 区域 1：数据导入
    with st.container(border=True):
        st.markdown("""
            <div class="card-header">
                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#0d9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="17 8 12 3 7 8"></polyline><line x1="12" y1="3" x2="12" y2="15"></line></svg>
                数据导入
            </div>
            """, unsafe_allow_html=True)

        tab_upload, tab_demo = st.tabs(["📤 上传数据文件", "🎲 生成示例数据"])

        with tab_upload:
            # 修改点 2: 开启多选功能 accept_multiple_files=True
            uploaded_files = st.file_uploader(
                "上传 CSV or Excel（支持多文件批量上传）",
                type=['csv', 'xlsx'],
                accept_multiple_files=True
            )

            st.caption("需要的数据列： `micro_test_name`, `test_result_other`, `inpatient_ward_name`, `采集时间`")

            if uploaded_files:
                # 显示即将处理的文件数量
                st.info(f"已选择 {len(uploaded_files)} 个文件等待处理")

                if st.button("确定导入并合并数据", type="primary"):
                    all_new_data = []
                    error_files = []

                    # 修改点 3: 添加进度条和状态文本
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    total_files = len(uploaded_files)

                    for i, file in enumerate(uploaded_files):
                        # 更新进度提示
                        status_text.text(f"正在读取文件 ({i + 1}/{total_files}): {file.name} ...")
                        progress_bar.progress((i + 1) / total_files)

                        try:
                            if file.name.endswith('.csv'):
                                df_temp = pd.read_csv(file)
                            else:
                                df_temp = pd.read_excel(file)

                            # 简单列名校验
                            required_cols = ['micro_test_name', 'test_result_other', '采集时间', 'inpatient_ward_name']
                            missing = [c for c in required_cols if c not in df_temp.columns]

                            if missing:
                                error_files.append(f"{file.name} (缺失列: {', '.join(missing)})")
                            else:
                                df_temp['采集时间'] = pd.to_datetime(df_temp['采集时间'], errors="coerce")
                                all_new_data.append(df_temp)

                        except Exception as e:
                            error_files.append(f"{file.name} (读取错误: {str(e)})")

                    status_text.empty()  # 清空状态文本

                    # 处理合并逻辑
                    if all_new_data:
                        # 1. 合并本次上传的所有文件
                        df_new_total = pd.concat(all_new_data, ignore_index=True)
                        df_new_total = clean_data(df_new_total)
                        new_count = len(df_new_total)

                        # 2. 获取旧数据 (如果存在)
                        if st.session_state['main_data'] is  None or st.session_state['main_data'].empty:
                            df_final = df_new_total.drop_duplicates()
                            duplicate_count = len(df_new_total) - len(df_final)
                        else:
                            # 修改点 1: 新旧数据合并
                            # 确保旧数据的时间列格式一致，防止去重失败
                            df_old = clean_data(st.session_state['main_data'])
                            # 找出共同列
                            common_cols = df_new_total.columns.intersection(df_old.columns).tolist()

                            # 合并
                            df_combined = pd.concat([df_old, df_new_total], ignore_index=True)

                            # 3. 去重：subset 只包含共同列
                            df_final = df_combined.drop_duplicates(subset=common_cols, keep='first')

                            duplicate_count = len(df_combined) - len(df_final)
                            # 假设 df_old 是你原来的大表，df_new 是那 500 行的表
                            # 确保两者都已经过 pd.to_datetime 处理了时间列

                            # 使用 merge 来查找差异，indicator=True 会告诉我们数据来源
                            merged = pd.merge(df_old, df_new_total, how='outer', indicator=True)

                            # 筛选出那些存在于 both (两个都有) 的，就是成功识别为重复的 (365行)
                            # 筛选出 right_only 的，就是新上传但没被认为是重复的 (那 135 行)
                            diff_rows = merged[merged['_merge'] == 'right_only']

                            print("以下行未能去重，请检查与原数据的微小差异：")
                            print(diff_rows.to_markdown(index=False))

                            # 进一步调试：取出其中一行新数据，和它在旧数据里对应的“双胞胎”做对比
                            # 比如打印出两者的 values 列表，肉眼对比


                        # 更新 Session State
                        st.session_state['main_data'] = df_final

                        # 结果反馈
                        msg = f"处理完成！本次读取 {new_count} 条数据。"
                        if duplicate_count > 0:
                            msg += f" 合并后自动去除了 {duplicate_count} 条重复数据。"

                        st.success(msg)

                        if error_files:
                            with st.expander("⚠️ 部分文件读取失败"):
                                for err in error_files:
                                    st.write(err)

                        time.sleep(1.5)  # 给用户一点时间看提示
                        st.rerun()
                    else:
                        st.warning("未能从上传的文件中读取到有效数据，请检查文件格式。")
                        if error_files:
                            with st.expander("查看错误详情"):
                                for err in error_files:
                                    st.write(err)

        with tab_demo:
            st.write("Generate synthetic data for testing purposes.")
            if st.button("Generate & Load Demo Data"):
                df = generate_micro_demo_data()
                st.session_state['main_data'] = df
                st.rerun()


    # # 区域 2：数据预览与统计 (仅当有数据时显示)
    # 注意：这里加了防空判断，防止 df 为 None
    if st.session_state.get('main_data') is not None and not st.session_state['main_data'].empty:
        df_current = st.session_state['main_data'].copy()

        # 确保处理函数存在，防止演示报错
        try:
            df_current["hospital_location"] = df_current["inpatient_ward_name"].apply(extract_hospital_location)
        except Exception:
            df_current["hospital_location"] = "未知"

        # 2. 处理采集时间
        df_current["datetime"] = pd.to_datetime(df_current["采集时间"], errors="coerce")

        with st.container(border=True):
            st.markdown("""
                <div class="card-header">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#0d9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line><polyline points="10 9 9 9 8 9"></polyline></svg>
                    数据概览
                </div>
                """, unsafe_allow_html=True)

            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            col_stat1.metric("总记录数", len(df_current))
            col_stat1.metric("院区个数", df_current['hospital_location'].nunique())

            # 防止时间列全空导致的报错
            if not df_current['datetime'].isna().all():
                min_date = str(df_current['datetime'].min().date())
                max_date = str(df_current['datetime'].max().date())
            else:
                min_date = "-"
                max_date = "-"

            col_stat2.metric("数据开始日期", min_date)
            col_stat2.metric("数据结束日期", max_date)

            st.divider()
            st.markdown("###### 数据预览 (前50条记录)")
            st.dataframe(df_current.head(50), use_container_width=True, hide_index=True)

            if st.button("🗑️ 清除所有数据", type="secondary"):
                st.session_state['main_data'] = pd.DataFrame()
                st.rerun()