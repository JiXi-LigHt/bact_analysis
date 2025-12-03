import pandas as pd
import random
from datetime import datetime, timedelta

def generate_micro_demo_data(num_patients=5, max_antibiotics_per_sample=8):
    """
    生成微生物耐药测试的 Demo 数据。

    Args:
        num_patients (int): 生成多少个病人的样本数据。
        max_antibiotics_per_sample (int): 每个样本包含的最大抗生素测试数量。

    Returns:
        pd.DataFrame: 符合要求的 DataFrame。
    """

    # --- 1. 基础字典配置 ---
    campuses = ['庆春', '之江', '城站', '余杭', '下沙']
    departments = ['呼吸内科', 'ICU', '感染科', '泌尿外科', '血液科', '综合监护室']
    surnames = ['赵', '钱', '孙', '李', '周', '吴', '郑', '王', '徐', '杨', '黄']

    bacteria_list = ['肺炎克雷伯菌', '铜绿假单胞菌', '大肠埃希菌', '鲍曼不动杆菌', '金黄色葡萄球菌', '粘质沙雷菌']

    antibiotics_map = {
        'general': ['阿米卡星', '庆大霉素', '左旋氧氟沙星', '环丙沙星', '复方新诺明', '替加环素'],
        'beta_lactam': ['氨曲南', '头孢哌酮/舒巴坦', '氨苄西林', '亚胺培南', '头孢吡肟', '头孢唑啉', '厄他培南',
                        '美罗培南', '头孢他啶', '哌拉西林/他唑巴坦'],
        'special': ['ESBL检测', '多粘菌素']
    }
    all_antibiotics = antibiotics_map['general'] + antibiotics_map['beta_lactam'] + antibiotics_map['special']

    columns = [
        'medical_record_no', 'patient_name', 'patient_sex', 'patient_birthday',
        'patient_age', 'patient_age_unit', 'inpatient_ward_name', 'sample_type_name',
        'sample_no', 'micro_test_name', 'test_name', 'test_result',
        'test_item_unit', 'test_method', 'test_result_other',
        '开单时间', '采集时间', '接收时间', '审核时间', 'Unnamed: 19'
    ]

    rows = []

    # --- 2. 循环生成数据 ---
    for _ in range(num_patients):
        # 2.1 生成病人及样本层级信息 (所有抗生素行共用这些信息)

        # 病人信息
        mrn = 2.3e9 + random.randint(100000, 999999)  # 模拟 2.30023e+09
        name = f"{random.choice(surnames)}**"
        sex = random.choice(['男', '女'])
        age = random.randint(20, 90)
        birth_year = 2025 - age
        birthday = f"{birth_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"

        # 院区构建 (关键要求)
        campus = random.choice(campuses)
        dept = random.choice(departments)
        ward_name = f"{dept}{random.randint(1, 10)}-{random.randint(1, 50)}({campus})"

        # 样本信息
        sample_type = random.choice(['痰', '血', '尿', '肺泡灌洗液'])
        # 生成形如 25060100XJ0005 的样本号
        date_part = "250601"
        sample_no = f"{date_part}00XJ{random.randint(1000, 9999)}"
        micro_bacteria = random.choice(bacteria_list)

        # 时间链逻辑
        base_time = datetime(2025, 5, 30) + timedelta(days=random.randint(0, 10))
        t_order = base_time + timedelta(hours=random.randint(8, 16))
        t_collect = t_order + timedelta(hours=random.randint(1, 12))
        t_receive = t_collect + timedelta(minutes=random.randint(30, 120))
        t_report = t_receive + timedelta(days=random.randint(2, 4))

        fmt = "%Y-%m-%d %H:%M:%S"

        # 2.2 生成该样本下的多条药敏结果
        # 随机选取几种抗生素
        selected_abx = random.sample(all_antibiotics, k=random.randint(3, max_antibiotics_per_sample))

        for abx in selected_abx:
            # 随机生成测试方法和单位
            method = random.choice(['mic', 'K-B法'])

            # 根据方法生成合理的数值格式
            if method == 'mic':
                unit = 'µg/ml'
                val_raw = random.choice([0.12, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64])
                operator = random.choice(['<=', '', '', '', '>='])
                result_val = f"{operator}{val_raw}"
            else:  # K-B法
                unit = 'mm'
                result_val = str(random.randint(6, 30))

            # 特殊处理 ESBL
            if abx == 'ESBL检测':
                result_val = random.choice(['Neg', 'Pos'])
                unit = 'nan'
                method = 'MIC法'
                susceptibility = '-'
            else:
                susceptibility = random.choice(['S', 'S', 'S', 'I', 'R'])  # 加大 S 的概率模拟真实情况

            row = {
                'medical_record_no': mrn,
                'patient_name': name,
                'patient_sex': sex,
                'patient_birthday': birthday,
                'patient_age': age,
                'patient_age_unit': '岁',
                'inpatient_ward_name': ward_name,
                'sample_type_name': sample_type,
                'sample_no': sample_no,
                'micro_test_name': micro_bacteria,
                'test_name': abx,
                'test_result': result_val,
                'test_item_unit': unit,
                'test_method': method,
                'test_result_other': susceptibility,
                '开单时间': t_order.strftime(fmt),
                '采集时间': t_collect.strftime(fmt),
                '接收时间': t_receive.strftime(fmt),
                '审核时间': t_report.strftime(fmt),
                'Unnamed: 19': ''
            }
            rows.append(row)

    # --- 3. 转换为 DataFrame ---
    df = pd.DataFrame(rows, columns=columns)

    return df