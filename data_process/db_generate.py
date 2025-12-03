import sqlite3
import random
import time
from datetime import datetime, timedelta

# ================= 配置项 =================
DB_PATH = "D:/sqlite/db/bact.db"
TOTAL_ROWS = 5_000_000  # 目标总行数
BATCH_SIZE = 50_000  # 批量提交的大小
START_DATE = datetime(2021, 1, 1)  # 4年跨度起始
END_DATE = datetime(2024, 12, 31)  # 4年跨度结束

# ================= 预定义数据池 =================
DEPARTMENTS = [
    ("呼吸内科", "庆春"), ("综合监护室", "庆春"), ("外科监护室", "庆春"),
    ("耳鼻喉科", "庆春"), ("泌尿外科", "下沙"), ("儿科", "滨江"),
    ("感染科", "庆春"), ("急诊科", "下沙")
]

BACTERIA_ANTIBIOTICS = {
    "肺炎克雷伯菌": ["氨曲南", "头孢哌酮/舒巴坦", "氨苄西林", "亚胺培南", "头孢吡肟", "头孢唑啉", "左旋氧氟沙星",
                     "复方新诺明"],
    "大肠埃希菌": ["头孢曲松", "庆大霉素", "哌拉西林/他唑巴坦", "厄他培南", "替加环素", "美罗培南", "阿米卡星"],
    "铜绿假单胞菌": ["环丙沙星", "头孢他啶", "多粘菌素", "妥布霉素", "阿兹夫定", "左氧氟沙星"],
    "金黄色葡萄球菌": ["青霉素", "苯唑西林", "红霉素", "克林霉素", "万古霉素", "利奈唑胺"],
    "鲍曼不动杆菌": ["米诺环素", "多西环素", "头孢哌酮/舒巴坦", "替加环素", "粘菌素"]
}

SAMPLES = ["痰", "血", "尿", "分泌物", "肺泡灌洗液"]
RESULTS_OTHER = ["S", "S", "S", "S", "R", "R", "I"]  # S多一些，R少一些
SURNAMES = list("赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜")
GENDERS = ["男", "女"]


# ================= 辅助函数 =================

def create_table(conn):
    """创建表结构"""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS micro_test_1")
    cursor.execute("""
    CREATE TABLE micro_test_1 (
        medical_record_no   REAL,
        patient_name        TEXT,
        patient_sex         TEXT,
        patient_birthday    TEXT,
        patient_age         INTEGER,
        patient_age_unit    TEXT,
        inpatient_ward_name TEXT,
        sample_type_name    TEXT,
        sample_no           TEXT,
        micro_test_name     TEXT,
        test_name           TEXT,
        test_result         TEXT,
        test_item_unit      TEXT,
        test_method         TEXT,
        test_result_other   TEXT,
        开单时间            TEXT,
        采集时间            TEXT,
        接收时间            TEXT,
        审核时间            TEXT,
        "Unnamed: 19"       TEXT,
        hospital_location   TEXT,
        datetime            TIMESTAMP,
        time_stamp          TIMESTAMP,
        date                TEXT
    );
    """)
    conn.commit()


def random_date(start, end):
    """生成指定范围内的随机时间"""
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_patient():
    """生成随机病人信息"""
    name = random.choice(SURNAMES) + "**"
    sex = random.choice(GENDERS)
    age = random.randint(18, 90)
    # 简单倒推生日
    birth_year = datetime.now().year - age
    birthday = f"{birth_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
    mrn = random.randint(1000000000, 9999999999)
    return mrn, name, sex, birthday, age


def generate_data():
    """生成数据的核心生成器"""
    conn = sqlite3.connect(DB_PATH)
    create_table(conn)
    cursor = conn.cursor()

    print(f"🚀 开始生成数据，目标: {TOTAL_ROWS} 行...")
    print(f"📅 时间跨度: {START_DATE.date()} 到 {END_DATE.date()}")

    start_time = time.time()
    rows_buffer = []
    generated_count = 0

    # 我们通过生成“样本”来生成“行”，因为一个样本包含多行抗生素
    while generated_count < TOTAL_ROWS:
        # 1. 生成样本级的基础信息 (Sample Level Info)
        base_time = random_date(START_DATE, END_DATE)
        date_str = base_time.strftime("%Y-%m-%d")
        time_str = base_time.strftime("%Y-%m-%d %H:%M:%S")

        # 模拟流程时间
        order_time = (base_time - timedelta(hours=random.randint(1, 12))).strftime("%Y-%m-%d %H:%M:%S")
        receive_time = (base_time + timedelta(hours=random.randint(1, 4))).strftime("%Y-%m-%d %H:%M:%S")
        audit_time = (base_time + timedelta(days=random.randint(2, 4))).strftime("%Y-%m-%d %H:%M:%S")

        # 病人与科室
        mrn, p_name, p_sex, p_bd, p_age = generate_patient()
        dept_name, loc_name = random.choice(DEPARTMENTS)
        ward_full = f"{dept_name}{random.randint(1, 15)}-{random.randint(1, 30)}({loc_name})"

        sample_type = random.choice(SAMPLES)
        # 模拟样本编号: YYMMDD + 随机码
        sample_no = base_time.strftime("%y%m%d") + "XJ" + f"{random.randint(1, 9999):04d}"

        # 2. 决定这个样本是什么细菌
        bacteria = random.choice(list(BACTERIA_ANTIBIOTICS.keys()))
        antibiotics = BACTERIA_ANTIBIOTICS[bacteria]

        # 3. 为该细菌生成多行抗生素结果 (Item Level Info)
        for abx in antibiotics:
            res_val = str(
                random.randint(1, 30)) if random.random() > 0.5 else f"<={random.choice([0.12, 0.25, 1, 2, 4, 8])}"
            res_flag = random.choice(RESULTS_OTHER)

            row = (
                mrn,  # medical_record_no
                p_name,  # patient_name
                p_sex,  # patient_sex
                p_bd,  # patient_birthday
                p_age,  # patient_age
                "岁",  # patient_age_unit
                ward_full,  # inpatient_ward_name
                sample_type,  # sample_type_name
                sample_no,  # sample_no
                bacteria,  # micro_test_name
                abx,  # test_name
                res_val,  # test_result
                random.choice(["mm", "µg/ml"]),  # test_item_unit
                random.choice(["K-B法", "mic"]),  # test_method
                res_flag,  # test_result_other
                order_time,  # 开单时间
                time_str,  # 采集时间
                receive_time,  # 接收时间
                audit_time,  # 审核时间
                "",  # Unnamed: 19
                loc_name,  # hospital_location
                time_str,  # datetime (使用采集时间)
                time_str,  # time_stamp (使用采集时间)
                date_str  # date
            )
            rows_buffer.append(row)
            generated_count += 1

        # 4. 批量插入
        if len(rows_buffer) >= BATCH_SIZE:
            cursor.executemany("""
                INSERT INTO micro_test_1 VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows_buffer)
            conn.commit()
            rows_buffer = []  # 清空缓存

            # 打印进度
            elapsed = time.time() - start_time
            speed = generated_count / elapsed
            print(f"已生成: {generated_count:,} 行 | 耗时: {elapsed:.2f}s | 速度: {speed:.0f} 行/秒")

            if generated_count >= TOTAL_ROWS:
                break

    # 插入剩余数据
    if rows_buffer:
        cursor.executemany("""
            INSERT INTO micro_test_1 VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows_buffer)
        conn.commit()

    # 创建索引 (对大数据量查询至关重要)
    print("正在创建索引 (这可能需要一点时间)...")
    cursor.execute("CREATE INDEX idx_datetime ON micro_test_1 (datetime)")
    cursor.execute("CREATE INDEX idx_location_bact ON micro_test_1 (hospital_location, micro_test_name)")
    conn.commit()

    conn.close()
    print(f"✅ 完成！共插入 {generated_count:,} 行数据到 {DB_PATH}")


if __name__ == "__main__":
    generate_data()