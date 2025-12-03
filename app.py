import streamlit as st
from matplotlib import rcParams
from streamlit_option_menu import option_menu
from page.dashboard import dashboard
from page.ris_analysis import ris_analysis_page
from page.trend_analysis import trend_analysis

# ==========================================
# 基础配置与 CSS 定制
# ==========================================
st.set_page_config(
    page_title="耐药菌分析系统",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="expanded"
)

# 设置中文字体
rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
rcParams['axes.unicode_minus'] = False

st.markdown("""
<style>
    /* 1. 基础清理 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    # header > div {
    #     display: none !important;
    # }
    .stApp { background-color: #f8fafc; }

    /* 2. 侧边栏样式 */
    [data-testid="stSidebar"] {
        background-color: #0f172a;
        border-right: 1px solid #1e293b;
    }
    [data-testid="stSidebar"] * { color: #e2e8f0; }

    /* === 3. 核心样式：统一所有卡片 (Form 和 Container) 的外观 === */
    /* 无论是顶部的 Form 还是下方的 Container，都使用统一的白底圆角阴影 */
    [data-testid="stForm"], 
    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: white;
        border-radius: 12px;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.06);
        border: 1px solid #e2e8f0;
        padding: 24px;
        margin-bottom: 20px;
    }

    /* 修复卡片内容贴底的问题，统一内边距 */
    [data-testid="stVerticalBlockBorderWrapper"] > div {
        padding-bottom: 20px !important;
    }

    /* 4. 标题样式 */
    .card-header {
        font-size: 16px;
        font-weight: 700;
        color: #0f172a;
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 20px;
        border-bottom: 1px solid #f1f5f9;
        padding-bottom: 15px;
    }

    .config-title {
        font-size: 16px;
        font-weight: 700;
        color: #0f172a;
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 20px;
        border-bottom: 1px solid #f1f5f9;
        padding-bottom: 15px;
    }

    /* 5. 院区 Header 样式 */
    .loc-header-box {
        background-color: #fff;
        border-bottom: 2px solid #f1f5f9;
        padding: 10px 0px;
        display: flex; justify-content: space-between; align-items: center;
        margin-bottom: 15px;
    }
    .loc-title { font-size: 14px; font-weight: 700; color: #334155; }
    .loc-badge { background: #fee2e2; color: #ef4444; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }

    /* 6. 卡片内容排版 */
    .card-content {
        display: flex;
        flex-direction: column;
        gap: 12px;
    }

    .bact-name { font-size: 15px; font-weight: 700; color: #1e293b; }
    .alert-date { 
        font-size: 12px; 
        color: #64748b; 
        background-color: #f1f5f9; 
        padding: 2px 8px; 
        border-radius: 4px; 
        margin-left: 8px;
        display: inline-block;
    }

    /* 标签样式 */
    .tag-row {
        display: flex;
        gap: 8px;
        margin-top: 4px;
    }
    .tag-pill { 
        display: inline-flex; align-items: center; 
        padding: 4px 10px; 
        border-radius: 6px; 
        font-size: 11px; 
        font-weight: 600; 
    }
    .tag-res { background-color: #fef2f2; color: #dc2626; border: 1px solid #fecaca; }
    .tag-cnt { background-color: #fffbeb; color: #d97706; border: 1px solid #fde68a; }

    /* 按钮样式 */
    button[kind="secondary"] {
        border: 1px solid #e2e8f0;
        background-color: #fff;
        color: #64748b;
        margin-top: 5px;
    }
    button[kind="secondary"]:hover {
        border-color: #cbd5e1;
        color: #334155;
        background-color: #f8fafc;
    }

    h5 { margin-bottom: 0px !important; }
    .helper-text { font-size: 12px; color: #94a3b8; font-weight: 400; margin-left: 5px; }
    /* 空状态样式 */
    .empty-state {
        text-align: center;
        padding: 40px;
        color: #64748b;
    }
</style>
""", unsafe_allow_html=True)



st.session_state['DB_PATH'] = st.secrets["database"]["path"]
st.session_state['SRC_TABLE'] = st.secrets["database"]["table"]


# ==========================================
# 侧边栏
# ==========================================
with st.sidebar:
    # Logo 区域
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px; padding-left: 5px;">
        <div>
            <div style="color: white; font-weight: bold; font-size: 16px;">耐药菌分析系统</div>
            <div style="color: #64748b; font-size: 11px;">时序分析与异常检测</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 渲染菜单
    selected_page = option_menu(
        menu_title=None,
        options=["信息面板", '耐药分析', "趋势分析"],
        icons=["speedometer2", "pie-chart", "graph-up-arrow"],
        menu_icon="cast",
        default_index=0,
        orientation="vertical",
        styles={
            "container": {"padding": "0!important", "background-color": "#0f172a", "border-radius": "0px"},
            "icon": {"color": "#94a3b8", "font-size": "14px"},
            "nav-link": {
                "font-size": "14px", "text-align": "left", "margin": "6px 0px",
                "color": "#cbd5e1", "background-color": "#0f172a",
            },
            "nav-link:hover": {"background-color": "#1e293b"},
            "nav-link-selected": {"background-color": "#0d9488", "color": "white", "font-weight": "600"},
        }
    )


# ==========================================
# 主页面内容
# ==========================================

if selected_page == "信息面板":
    dashboard()
elif selected_page == "耐药分析":
    ris_analysis_page()
elif selected_page == "趋势分析":
    trend_analysis()