import re
import pandas as pd

# 提取院区名称函数
def extract_hospital_location(ward_name):
    match = re.search(r'\((.*?)\)', str(ward_name))
    return match.group(1) if match else "未知院区"

# 获取耐药菌耐药性数据
def get_resistance_df(df):
    df["hospital_location"] = df["inpatient_ward_name"].apply(extract_hospital_location)

    # 2. 处理采集时间：转换为datetime类型（保留完整时间戳），并提取日期（用于分组）
    df["datetime"] = pd.to_datetime(df["采集时间"], errors="coerce")  # 完整时间戳（判断唯一的依据）
    resistance_df = (
        df.groupby(["datetime", "micro_test_name", "hospital_location"])
        ["test_result_other"]
        .apply(lambda x: (x.isin(["R", "+"])).mean() * 100)
        .reset_index()
        .round(2)
    )

    resistance_df.rename(columns={"test_result_other": "resistance_rate"}, inplace=True)
    return resistance_df

# 获取耐药菌样本数量数据
def get_count_df(df):
    df["hospital_location"] = df["inpatient_ward_name"].apply(extract_hospital_location)

    # 2. 处理采集时间：转换为datetime类型（保留完整时间戳），并提取日期（用于分组）
    df["time_stamp"] = pd.to_datetime(df["采集时间"], errors="coerce")  # 完整时间戳（判断唯一的依据）
    df["date"] = df["time_stamp"].dt.strftime("%Y-%m-%d")  # 日期（分组用）

    # 3. 按「日期+微生物+院区」分组，统计每组内的「唯一时间戳个数」
    # nunique()：统计非重复值的数量（即唯一时间戳个数）
    count_df = df.groupby(
        ["date", "micro_test_name", "hospital_location"],  # 分组键
        as_index=False
    )["time_stamp"].nunique().rename(columns={"time_stamp": "daily_count"})

    count_df["date"] = pd.to_datetime(count_df["date"])
    return count_df