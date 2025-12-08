# 部署手册

## 环境要求：

使用 `Anaconda` 或 `Miniconda` 进行部署

## 部署步骤

1. 运行 `git clone https://github.com/JiXi-LigHt/bact_analysis.git` 获取项目

2. `Windows` 需要打开 `Anaconda Prompt` 才能运行 `conda` 命令。 `Linux `和 `macOS` 需要配置好 `conda` 环境变量。

3. 进入项目根目录。`Windows` 需要在 `Anaconda Prompt` 中进行全部操作。

4. （可选）配置conda源

   ```
   # 1. 恢复默认设置
   conda config --remove-key channels
   
   # 2. 添加清华源的 main 和 free 仓库
   conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/
   conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
   
   # 3. 添加 conda-forge 镜像
   conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud/conda-forge/
   
   # 4. 设置搜索时显示通道地址
   conda config --set show_channel_urls yes
   ```

5. 运行命令 `conda env create -f environment.yml`

6. 运行命令 `conda activate bact_analysis`

7. 将data文件夹中的数据表转为sqlite数据库文件，通过修改`db_handler.py`中下面代码可以自定义读取的数据表文件和存储数据库名、表名、数据库文件存储路径。

   ```python
       # data_process.db_handler.py
       excel_filename = PROJECT_ROOT / 'data' / 'whonet6_cleaned.xlsx'
       db_filename = PROJECT_ROOT / 'data' / 'bact.db'
       table_name = 'micro_test'
   ```

   随后运行命令 `python .\data_process\db_handler.py` 可以在data目录中创建sqlite数据库文件 `bact.db` 


7. 数据库中需要数据表格式如下，在原始数据表上增加了 'hospital_location', 'datetime', 'time_stamp', 'date'以便于分析。

   ```
   ['medical_record_no', 'patient_name', 'patient_sex', 'patient_birthday', 'patient_age', 'patient_age_unit', 'inpatient_ward_name', 'sample_type_name', 'sample_no', 'micro_test_name', 'test_name', 'test_result', 'test_it_unit', 'test_method', 'test_result_other', '开单时间', '采集时间', '接收时间', '审核时间', 'Unnamed: 19', 'hospital_location', 'datetime', 'time_stamp', 'date']
   ```

8. 提供与 `whonet6_cleaned.xlsx` 格式一致的xlsx数据文件可以经过 `db_handler.py` 自动创建该形式的sqlite数据表。

9. 修改 `.streamlit\secrets.toml` 文件来指定数据来源（path是基于项目根目录的相对路径或使用完整的绝对路径）。

   ```
   [database]
   path = "data/bact.db"
   table = "micro_test"
   ```

10. 运行下面命令即可启动应用`streamlit run .\app.py`

## 一些问题

1. 数据库格式问题可以跟我详细讲解，目前支持从提供的 `whonet6_cleaned.xlsx` 的格式一致的表格文件生成我自定义格式的sqlite数据库文件。

   现有数据读取逻辑大概率是不支持医院现有的表格式的，因为我在 `whonet6_cleaned.xlsx` 的格式基础上加了一些列。

   可以先测试部署流程。

   若已有数据库和表，请提供表的 `schema` ，我可以进行相应的修改以适配目前已有的数据源。

   