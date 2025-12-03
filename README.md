在项目根目录执行以下操作以启动程序

运行命令安装依赖

`pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple`

数据示例可以运行db_handler可以将data文件夹的示例表格存为sqlite，通过修改`db_handler.py`中下面代码可以自定义读取的数据表文件和存储数据库名、表名。
```python
    # data_process.db_handler.py
    excel_filename = PROJECT_ROOT / 'data' / 'whonet6_cleaned.xlsx'
    db_filename = PROJECT_ROOT / 'data' / 'bact.db'
    table_name = 'micro_test'
```

随后运行下面命令可以在data目录中创建sqlite数据库文件
`python .\data_process\db_handler.py`

数据库中需要数据表格式如下，在原始数据表上增加了 'hospital_location', 'datetime', 'time_stamp', 'date'以便于分析。所提供xlsx数据文件经过db_handler可以自动创建如下形式的sqlite数据表。

['medical_record_no', 'patient_name', 'patient_sex', 'patient_birthday', 'patient_age', 'patient_age_unit', 'inpatient_ward_name', 'sample_type_name', 'sample_no', 'micro_test_name', 'test_name', 'test_result', 'test_it_unit', 'test_method', 'test_result_other', '开单时间', '采集时间', '接收时间', '审核时间', 'Unnamed: 19', 'hospital_location', 'datetime', 'time_stamp', 'date']


修改`.streamlit\secrets.toml`文件来指定数据来源（path是基于项目根目录的相对路径）。
```
[database]
path = "data/bact.db"
table = "micro_test"
```

运行下面命令即可启动应用

`streamlit run .\app.py`

