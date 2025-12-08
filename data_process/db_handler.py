import sys
from pathlib import Path
import pandas as pd
import sqlite3

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_process.data_processer import extract_hospital_location


# ==========================================
# 核心逻辑 - 读取 Excel 并存入 SQLite
# ==========================================
def excel_to_sqlite(excel_file, db_name, table_name):
    try:
        # 1. 使用 Pandas 读取 Excel 文件
        print(f"正在读取 {excel_file} ...")
        df = pd.read_excel(excel_file)

        # 如果是CSV文件，使用: df = pd.read_csv(excel_file)

        # 2. 连接到 SQLite 数据库
        # 如果数据库不存在，会自动创建
        conn = sqlite3.connect(db_name)


        try:
            df["hospital_location"] = df["inpatient_ward_name"].apply(extract_hospital_location)
        except Exception:
            df["hospital_location"] = "未知院区"
        df["datetime"] = pd.to_datetime(df["采集时间"], errors="coerce")

        # 2. 处理采集时间：转换为datetime类型（保留完整时间戳），并提取日期（用于分组）
        df["time_stamp"] = pd.to_datetime(df["采集时间"], errors="coerce")  # 完整时间戳（判断唯一的依据）
        df["date"] = df["time_stamp"].dt.strftime("%Y-%m-%d")  # 日期（分组用）

        # 3. 将 DataFrame 写入 SQL
        # if_exists 参数说明:
        # 'fail': 如果表存在，什么都不做（抛出错误）
        # 'replace': 如果表存在，删除旧表，创建新表
        # 'append': 如果表存在，将数据追加到后面
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

        print(f"✅ 成功将数据存入数据库 '{db_name}' 的表 '{table_name}' 中。")

        # 4. 关闭连接
        conn.close()

    except Exception as e:
        print(f"❌ 发生错误: {e}")


# ==========================================
# 验证 - 从数据库读取数据并打印
# ==========================================
def verify_data(db_name, table_name):
    print("\n--- 正在验证数据库内容 ---")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 查询数据
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # 获取列名
    names = [description[0] for description in cursor.description]
    print(f"列名: {names}")

    conn.close()

# ==========================================
# 主程序执行入口
# ==========================================
if __name__ == "__main__":
    # 定义文件名
    excel_filename = PROJECT_ROOT / 'data' / 'whonet6_cleaned.xlsx'
    db_filename = PROJECT_ROOT / 'data' / 'bact.db'
    table_name = 'micro_test'
    # 2. 执行转换
    excel_to_sqlite(excel_filename, db_filename, table_name)

    # 3. 验证结果
    verify_data(db_filename, table_name)