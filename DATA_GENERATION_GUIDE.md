# 测试数据生成与导入指南

本文档说明如何使用提供的脚本生成测试数据并导入到OpenGauss数据库。

## 目录

- [环境准备](#环境准备)
- [数据库初始化](#数据库初始化)
- [数据生成](#数据生成)
- [数据过滤](#数据过滤)
- [参数配置说明](#参数配置说明)
- [数据导入OpenGauss](#数据导入opengauss)
- [验证数据](#验证数据)
- [常见问题](#常见问题)

---

## 环境准备

### 1. Python环境

确保已安装Python 3.7或更高版本：

```bash
python3 --version
```

不需要安装额外的Python包，脚本只使用标准库。

### 2. OpenGauss数据库

确保OpenGauss数据库已安装并运行。

检查连接：

```bash
gsql -d postgres -p 5432 -U username
```

---

## 数据库初始化

### 步骤1：执行初始化SQL脚本

使用`init_database.sql`脚本创建数据库和表结构。

**方法1：通过gsql命令行工具**

```bash
gsql -d postgres -p 5432 -U username -f init_database.sql
```

**方法2：在gsql交互环境中执行**

```bash
# 进入gsql
gsql -d postgres -p 5432 -U username

# 执行脚本
\i /path/to/init_database.sql
```

**注意事项：**

- 脚本会删除已存在的`antifraud_test`数据库，请确认数据安全
- 如果数据库名称需要修改，请编辑`init_database.sql`第7行
- 脚本执行完成后，会自动创建7个表（4个原始表 + 3个聚合表）

### 步骤2：验证表创建

连接到新创建的数据库并检查表：

```bash
# 连接到antifraud_test数据库
gsql -d antifraud_test -p 5432 -U username

# 列出所有表
\dt

# 查看表结构示例
\d ty_m_unreal_person_call_data_filter
```

应该能看到以下7个表：

**原始数据表：**
1. `ty_m_unreal_person_call_data_filter` - 用户通话交际圈汇总日表
2. `ty_m_unreal_person_user_number_data_filter` - 用户基础信息日表及月表
3. `tv_m_cust_single_flux_used` - 单一用户流量信息月表
4. `tw_d_is_pack_users_new_used` - 资费套餐订购日明细表

**聚合数据表：**
5. `user_graph` - 用户信息（用于构建点）
6. `call_graph` - 通话关系（用于构建边）
7. `tv_analysis` - 宽表（用于分析用户特征变化）

---

## 数据生成

### 步骤1：运行数据生成脚本

直接运行Python脚本：

```bash
python3 generate_test_data.py
```

### 步骤2：等待数据生成完成

脚本会显示生成进度：

```
============================================================
移动反欺诈测试数据生成工具
============================================================

当前配置:
  总用户数: 10,000
  生成天数: 30
  每用户每天通话次数: 10
  正常用户比例: 70.0%
  可疑用户比例: 20.0%
  欺诈用户比例: 10.0%
  输出目录: ./output_data

开始生成 10000 个用户...
用户生成完成！正常用户: 7000, 可疑用户: 2000, 欺诈用户: 1000
开始生成通话关系...
通话关系生成完成！
开始导出数据到CSV文件...
  ✓ 用户基础信息表已导出: ./output_data/ty_m_unreal_person_user_number_data_filter.csv
  ✓ 通话记录表已导出: ./output_data/ty_m_unreal_person_call_data_filter.csv
  ✓ 流量信息表已导出: ./output_data/tv_m_cust_single_flux_used.csv
  ✓ 套餐信息表已导出: ./output_data/tw_d_is_pack_users_new_used.csv
数据导出完成！输出目录: ./output_data

============================================================
数据生成完成！
============================================================
```

### 步骤3：检查生成的文件

生成的CSV文件位于`./output_data`目录：

```bash
ls -lh output_data/
```

应该能看到4个CSV文件：
- `ty_m_unreal_person_call_data_filter.csv` - 通话记录
- `ty_m_unreal_person_user_number_data_filter.csv` - 用户基础信息
- `tv_m_cust_single_flux_used.csv` - 流量信息
- `tw_d_is_pack_users_new_used.csv` - 套餐信息

---

## 数据过滤

生成的原始CSV数据需要转换为目标格式的TXT文件（user.txt、call.txt、tv.txt），这些文件可以直接用于图分析。

### 步骤1：运行数据过滤脚本

```bash
python3 filter_data.py
```

### 步骤2：查看生成结果

过滤后的数据位于`./filter_data`目录：

```bash
ls -lh filter_data/
```

应该能看到3个TXT文件：
- `user.txt` - 用户信息（用于构建点）
- `call.txt` - 通话关系（用于构建边）
- `tv.txt` - 用户特征宽表（用于业务分析）

### 数据映射说明

#### 1. user.txt（7个字段）

| 字段名 | 字段含义 | 来源 |
|--------|---------|------|
| MSISDN | 手机号码 | 用户基础信息表.MSISDN |
| USER_ID | 用户唯一标识 | 用户基础信息表.USER_ID |
| NEW_RCN_ID | 是否新入网用户 | 根据RCN_DATE计算（≤2周=1） |
| RCN_DURA | 入网时长（月） | 根据RCN_DATE和STATIS_YMD计算 |
| IDTY_NBR | 身份证号 | 用户基础信息表.IDTY_CODE |
| STATIS_YMD | 统计日期 | 用户基础信息表.STATIS_YMD |
| PROV_ID | 省份编码 | 用户基础信息表.PROV_ID |

#### 2. call.txt（4个字段）

| 字段名 | 字段含义 | 来源 |
|--------|---------|------|
| MSISDN | 主叫手机号 | 通话记录表.MSISDN |
| OPP_MSISDN | 被叫手机号 | 通话记录表.OPP_MSISDN |
| STATIS_YMD | 通话日期 | 通话记录表.STATIS_YMD |
| PROV_ID | 省份编码 | 通话记录表.PROV_ID |

**注意：** call.txt会自动去重，同一天的同一对通话关系只保留一次。

#### 3. tv.txt（34个字段）

tv.txt是一个宽表，包含34个字段，来源于3个表的联合：

**用户基础属性（20个字段）** - 来源：用户基础信息表
- USER_ID, MSISDN, IS_PRETTY_NUM, PRETTY_NUM_TYP, VIP_CUST_ID, VIP_LVL
- AGE_LVL, SEX, OCPN_CODE, EDUCAT_DEGREE_CODE, BRAND_ID, RCN_CHNL_TYP
- IS_CAMP_USER, IS_CAMP_AREA_USER, IS_GROUP_USER, MEMB_TYP, IS_GROUP_KEY_INDV
- IS_GSM_USER, GSM_USER_LVL, INNET_DURA_LVL_CODE, USER_AREA_BELO

**流量信息（9个字段）** - 来源：流量信息表
- ONNET_ALL_FLUX, WDAY_ONNET_FLUX, NWDAY_ONNET_FLUX
- ONNET_FLUX_3G, ONNET_FLUX_4G, TOT_FLUX_5G
- FLUX_FEE, FLUX_FEE_4G, FLUX_TOT_FEE_5G

**套餐信息（4个字段）** - 来源：套餐信息表
- CHARGE_PACKAGE_UNIFY_CODE, CHARGE_PACKAGE_TYP, PACK_MON, PACKAGE_5G_ID

### 数据格式

**默认格式（与cmcc/data一致）：**
- **分隔符：** €€（欧元符号的两个）
- **编码：** UTF-8
- **表头：** 无表头，直接是数据行
- **数据行：** 纯数据记录

### 示例数据

**user.txt示例：**
```
ecada95de963d211cccb523efb728969€€21d6a36957ef55162b68fc93c2714ddc€€1€€11€€37693cfc748049e45d87b8c7d8b9aacd€€20220718€€10000
df14653939d07a00159b2f756cb63799€€9662acfbd200cd57f95c5a753829cb90€€0€€59€€8f14e45fceea167a5a36dedd4bea2543€€20220718€€10000
```

**call.txt示例：**
```
2b29f13d756036e94e8d1942f36ad9e6€€53c28528e5781267f8ad404a35e90401€€20220718€€10000
e2b1a4c426c125d8a4f0b8360871a452€€3d8dc8fc8d8c695987a88667b0d218d6€€20220718€€10000
```

### 配置说明

如果需要修改输入/输出目录、分隔符或表头，编辑`filter_data.py`文件中的`FilterConfig`类：

```python
class FilterConfig:
    INPUT_DIR = "./output_data"         # 输入目录
    OUTPUT_DIR = "./filter_data"        # 输出目录

    # 输出格式配置（默认与cmcc/data格式一致）
    OUTPUT_DELIMITER = '€€'            # 分隔符（默认：€€）
    OUTPUT_WITH_HEADER = False         # 是否输出表头（默认：False）
```

**可选配置示例：**

如果需要生成带表头的Tab分隔文件（适合Excel打开）：

```python
OUTPUT_DELIMITER = '\t'                # 改为制表符
OUTPUT_WITH_HEADER = True              # 改为输出表头
```

如果需要生成CSV格式：

```python
OUTPUT_DELIMITER = ','                 # 改为逗号
OUTPUT_WITH_HEADER = True              # 改为输出表头
```

---

## 参数配置说明

### 修改数据生成参数

所有配置参数都在`generate_test_data.py`文件的`Config`类中（第18-75行）。

打开文件并修改以下参数：

```python
class Config:
    """数据生成配置"""

    # ===== 数据规模配置 =====
    TOTAL_USERS = 10000              # 总用户数（建议：10000-1000000）
    CALLS_PER_USER_PER_DAY = 10      # 每个用户每天平均通话次数（建议：5-20）
    DAYS_TO_GENERATE = 30            # 生成多少天的数据（建议：30）

    # ===== 用户画像比例 =====
    NORMAL_USER_RATIO = 0.70         # 正常用户比例 70%
    SUSPICIOUS_USER_RATIO = 0.20     # 可疑用户比例 20%
    FRAUD_USER_RATIO = 0.10          # 欺诈用户比例 10%

    # ===== 新入网用户比例 =====
    NEW_USER_RATIO = 0.10            # 新入网用户（≤2周）比例 10%
    RECENT_USER_RATIO = 0.20         # 近期用户（2-4周）比例 20%
    OLD_USER_RATIO = 0.70            # 历史用户（>1个月）比例 70%
```

### 关键参数说明

| 参数名称 | 说明 | 默认值 | 建议范围 |
|---------|------|--------|---------|
| `TOTAL_USERS` | 总用户数 | 10,000 | 10,000 - 1,000,000 |
| `CALLS_PER_USER_PER_DAY` | 每用户每天通话次数 | 10 | 5 - 20 |
| `DAYS_TO_GENERATE` | 生成数据的天数 | 30 | 7 - 90 |
| `NORMAL_USER_RATIO` | 正常用户比例 | 0.70 | 0.5 - 0.8 |
| `SUSPICIOUS_USER_RATIO` | 可疑用户比例 | 0.20 | 0.1 - 0.3 |
| `FRAUD_USER_RATIO` | 欺诈用户比例 | 0.10 | 0.05 - 0.2 |

### 数据规模估算

根据配置参数，可以估算生成的数据量：

**通话记录数量计算：**
```
通话记录数 = TOTAL_USERS × CALLS_PER_USER_PER_DAY × DAYS_TO_GENERATE
```

**示例：**
- 10,000用户 × 10通话/天 × 30天 = **3,000,000条通话记录**
- 100,000用户 × 10通话/天 × 30天 = **30,000,000条通话记录**

**内存和时间预估：**
- 10,000用户：生成时间约30秒，CSV文件约500MB
- 100,000用户：生成时间约5分钟，CSV文件约5GB
- 1,000,000用户：生成时间约50分钟，CSV文件约50GB

---

## 数据导入OpenGauss

### 方法1：使用COPY命令（推荐，速度快）

#### 1. 准备导入脚本

创建一个导入脚本`import_data.sql`：

```sql
-- 连接到antifraud_test数据库
\c antifraud_test;

-- 导入用户基础信息
COPY ty_m_unreal_person_user_number_data_filter FROM '/path/to/output_data/ty_m_unreal_person_user_number_data_filter.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- 导入通话记录
COPY ty_m_unreal_person_call_data_filter FROM '/path/to/output_data/ty_m_unreal_person_call_data_filter.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- 导入流量信息
COPY tv_m_cust_single_flux_used FROM '/path/to/output_data/tv_m_cust_single_flux_used.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- 导入套餐信息
COPY tw_d_is_pack_users_new_used FROM '/path/to/output_data/tw_d_is_pack_users_new_used.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
```

**注意：** 请将`/path/to/output_data/`替换为实际的绝对路径。

#### 2. 执行导入

```bash
gsql -d antifraud_test -p 5432 -U username -f import_data.sql
```

### 方法2：使用\\copy命令（客户端导入）

如果服务器无法直接访问CSV文件，使用`\copy`命令：

```bash
# 进入gsql
gsql -d antifraud_test -p 5432 -U username

# 执行导入（在gsql中）
\copy ty_m_unreal_person_user_number_data_filter FROM '/path/to/output_data/ty_m_unreal_person_user_number_data_filter.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy ty_m_unreal_person_call_data_filter FROM '/path/to/output_data/ty_m_unreal_person_call_data_filter.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy tv_m_cust_single_flux_used FROM '/path/to/output_data/tv_m_cust_single_flux_used.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

\copy tw_d_is_pack_users_new_used FROM '/path/to/output_data/tw_d_is_pack_users_new_used.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
```

### 方法3：使用GDS（Gauss Data Service）批量导入

对于大规模数据（>10GB），建议使用GDS工具，性能更好。

请参考OpenGauss官方文档：
https://docs.opengauss.org/zh/docs/latest/docs/Developerguide/GDS%E5%AF%BC%E5%85%A5.html

---

## 验证数据

### 1. 检查导入的数据量

```sql
-- 连接到数据库
\c antifraud_test;

-- 检查各表的记录数
SELECT 'ty_m_unreal_person_call_data_filter' AS table_name, COUNT(*) AS row_count
FROM ty_m_unreal_person_call_data_filter
UNION ALL
SELECT 'ty_m_unreal_person_user_number_data_filter', COUNT(*)
FROM ty_m_unreal_person_user_number_data_filter
UNION ALL
SELECT 'tv_m_cust_single_flux_used', COUNT(*)
FROM tv_m_cust_single_flux_used
UNION ALL
SELECT 'tw_d_is_pack_users_new_used', COUNT(*)
FROM tw_d_is_pack_users_new_used;
```

### 2. 抽样检查数据质量

```sql
-- 查看用户基础信息样例
SELECT * FROM ty_m_unreal_person_user_number_data_filter LIMIT 5;

-- 查看通话记录样例
SELECT MSISDN, OPP_MSISDN, STATIS_YMD, CALL_DURA_SEC
FROM ty_m_unreal_person_call_data_filter LIMIT 10;

-- 检查新入网用户数量
SELECT COUNT(*) AS new_users
FROM ty_m_unreal_person_user_number_data_filter
WHERE IS_CM_NADD = '1';

-- 检查同一身份证多个号码的情况
SELECT IDTY_CODE, SAME_IDCARD_MSISDN_CNT, COUNT(*) AS actual_count
FROM ty_m_unreal_person_user_number_data_filter
GROUP BY IDTY_CODE, SAME_IDCARD_MSISDN_CNT
HAVING COUNT(*) > 1
LIMIT 10;
```

### 3. 生成聚合表数据（可选）

如果需要生成`user_graph`、`call_graph`和`tv_analysis`聚合表，执行以下SQL：

```sql
-- 生成user_graph表
INSERT INTO user_graph (MSISDN, USER_ID, NEW_RCN_ID, RCN_DURA, IDTY_NBR, STATIS_YMD, PROV_ID)
SELECT
    MSISDN,
    USER_ID,
    CASE WHEN INNET_DURA <= 0.5 THEN '1' ELSE '0' END AS NEW_RCN_ID,
    CAST(INNET_DURA AS VARCHAR) AS RCN_DURA,
    IDTY_CODE AS IDTY_NBR,
    STATIS_YMD,
    PROV_ID
FROM ty_m_unreal_person_user_number_data_filter;

-- 生成call_graph表（去重）
INSERT INTO call_graph (MSISDN, OPP_MSISDN, STATIS_YMD, PROV_ID)
SELECT DISTINCT MSISDN, OPP_MSISDN, STATIS_YMD, PROV_ID
FROM ty_m_unreal_person_call_data_filter;

-- 生成tv_analysis表
INSERT INTO tv_analysis (
    USER_ID, MSISDN, IS_PRETTY_NUM, PRETTY_NUM_TYP, VIP_CUST_ID, VIP_LVL,
    AGE_LVL, SEX, OCPN_CODE, EDUCAT_DEGREE_CODE, BRAND_ID, RCN_CHNL_TYP,
    IS_CAMP_USER, IS_CAMP_AREA_USER, IS_GROUP_USER, MEMB_TYP, IS_GROUP_KEY_INDV,
    IS_GSM_USER, GSM_USER_LVL, INNET_DURA_LVL_CODE, USER_AREA_BELO,
    ONNET_ALL_FLUX, WDAY_ONNET_FLUX, NWDAY_ONNET_FLUX, ONNET_FLUX_3G,
    ONNET_FLUX_4G, TOT_FLUX_5G, FLUX_FEE, FLUX_FEE_4G, FLUX_TOT_FEE_5G,
    CHARGE_PACKAGE_UNIFY_CODE, CHARGE_PACKAGE_TYP, PACK_MON, PACKAGE_5G_ID
)
SELECT
    u.USER_ID, u.MSISDN, u.IS_PRETTY_NUM, u.PRETTY_NUM_TYP, u.VIP_CUST_ID, u.VIP_LVL,
    u.AGE_LVL, u.SEX, u.OCPN_CODE, u.EDUCAT_DEGREE_CODE, u.BRAND_ID, u.RCN_CHNL_TYP,
    u.IS_CAMP_USER, u.IS_CAMP_AREA_USER, u.IS_GROUP_USER, u.MEMB_TYP, u.IS_GROUP_KEY_INDV,
    u.IS_GSM_USER, u.GSM_USER_LVL, u.INNET_DURA_LVL_CODE, u.USER_AREA_BELO,
    CAST(f.ONNET_ALL_FLUX AS VARCHAR), CAST(f.WDAY_ONNET_FLUX AS VARCHAR),
    CAST(f.NWDAY_ONNET_FLUX AS VARCHAR), CAST(f.ONNET_FLUX_3G AS VARCHAR),
    CAST(f.ONNET_FLUX_4G AS VARCHAR), CAST(f.TOT_FLUX_5G AS VARCHAR),
    CAST(f.FLUX_FE AS VARCHAR), CAST(f.FLUX_FEE_4G AS VARCHAR),
    CAST(f.FLUX_TOT_FEE_5G AS VARCHAR),
    p.CHARGE_PACKAGE_UNIFY_CODE, p.CHARGE_PACKAGE_TYP, p.PACK_MON, p.PACKAGE_5G_ID
FROM ty_m_unreal_person_user_number_data_filter u
LEFT JOIN tv_m_cust_single_flux_used f ON u.MSISDN = f.MSISDN
LEFT JOIN (
    SELECT MSISDN,
           MAX(CHARGE_PACKAGE_UNIFY_CODE) AS CHARGE_PACKAGE_UNIFY_CODE,
           MAX(CHARGE_PACKAGE_TYP) AS CHARGE_PACKAGE_TYP,
           MAX(PACK_MON) AS PACK_MON,
           MAX(PACKAGE_5G_ID) AS PACKAGE_5G_ID
    FROM tw_d_is_pack_users_new_used
    GROUP BY MSISDN
) p ON u.MSISDN = p.MSISDN;
```

---

## 常见问题

### 1. 生成数据时内存不足

**解决方法：**
- 减少`TOTAL_USERS`参数
- 减少`DAYS_TO_GENERATE`参数
- 分批生成数据（修改脚本多次运行）

### 2. CSV文件导入时编码错误

**解决方法：**
- 确保CSV文件使用UTF-8编码
- 在COPY命令中指定`ENCODING 'UTF8'`

### 3. 导入时权限错误

**解决方法：**
- 确保数据库用户有INSERT权限
- 确保CSV文件路径对数据库服务器可读
- 使用`\copy`命令而不是`COPY`命令

### 4. 数据生成速度慢

**解决方法：**
- 减少`CALLS_PER_USER_PER_DAY`参数
- 使用更快的硬盘（SSD）
- 考虑使用多线程版本的脚本（需要修改代码）

### 5. 如何生成不同日期的数据

**解决方法：**

修改`generate_test_data.py`中的`Config.END_DATE`参数：

```python
# 原来：使用当前日期
END_DATE = datetime.now().strftime("%Y%m%d")

# 修改为：使用指定日期
END_DATE = "20220718"  # 格式：YYYYMMDD
```

### 6. 如何增加特定类型用户的比例

**解决方法：**

修改`Config`类中的用户比例参数，例如增加欺诈用户比例：

```python
NORMAL_USER_RATIO = 0.60         # 正常用户 60%
SUSPICIOUS_USER_RATIO = 0.20     # 可疑用户 20%
FRAUD_USER_RATIO = 0.20          # 欺诈用户 20%（从10%提高到20%）
```

**注意：** 三个比例之和必须等于1.0

---

## 技术支持

如有问题，请检查：
1. Python版本是否 >= 3.7
2. OpenGauss数据库连接是否正常
3. CSV文件路径是否正确
4. 数据库用户权限是否充足

---

## 附录：快速开始示例

### 方式1：导入到OpenGauss数据库

```bash
# 1. 初始化数据库
gsql -d postgres -p 5432 -U username -f init_database.sql

# 2. 生成测试数据（使用默认配置）
python3 generate_test_data.py

# 3. 导入数据（创建import_data.sql后执行）
gsql -d antifraud_test -p 5432 -U username -f import_data.sql

# 4. 验证数据
gsql -d antifraud_test -p 5432 -U username -c "SELECT COUNT(*) FROM ty_m_unreal_person_call_data_filter;"
```

### 方式2：生成TXT文件用于图分析

```bash
# 1. 生成原始CSV数据
python3 generate_test_data.py

# 2. 过滤转换为TXT格式
python3 filter_data.py

# 3. 查看生成的文件
ls -lh filter_data/
# 输出：user.txt, call.txt, tv.txt

# 4. 查看user.txt的前几行
head -n 5 filter_data/user.txt

# 5. 统计各文件的行数
wc -l filter_data/*.txt
```

### 完整流程（数据库 + TXT文件）

```bash
# 步骤1：生成测试数据
python3 generate_test_data.py

# 步骤2：过滤为TXT格式（用于图分析）
python3 filter_data.py

# 步骤3：初始化数据库
gsql -d postgres -p 5432 -U username -f init_database.sql

# 步骤4：导入到数据库（可选）
gsql -d antifraud_test -p 5432 -U username -f import_data.sql

# 现在你有了：
# - filter_data/ 目录下的TXT文件（用于图分析）
# - OpenGauss数据库中的完整数据（用于SQL分析）
```

完成！现在可以开始使用测试数据进行分析了。
