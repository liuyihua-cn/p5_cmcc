# 脚本使用总览

本文档简要说明各个脚本的用途和使用顺序。

## 脚本清单

| 脚本文件 | 用途 | 输入 | 输出 |
|---------|------|------|------|
| `init_database.sql` | 初始化OpenGauss数据库 | 无 | 创建数据库和7个表 |
| `generate_test_data.py` | 生成测试数据 | 无 | output_data/*.csv (4个CSV文件) |
| `filter_data.py` | 过滤转换数据格式 | output_data/*.csv | filter_data/*.txt (3个TXT文件) |

## 使用流程

### 快速开始（生成TXT文件）

如果你只需要TXT格式的数据用于图分析：

```bash
# 1. 生成原始数据
python3 generate_test_data.py

# 2. 转换为TXT格式
python3 filter_data.py

# 完成！数据在 filter_data/ 目录
```

### 完整流程（数据库 + TXT文件）

如果你需要同时将数据导入数据库：

```bash
# 1. 初始化数据库
gsql -d postgres -p 5432 -U username -f init_database.sql

# 2. 生成原始数据
python3 generate_test_data.py

# 3. 转换为TXT格式
python3 filter_data.py

# 4. 导入到数据库（需先创建import_data.sql）
gsql -d antifraud_test -p 5432 -U username -f import_data.sql
```

## 文件说明

### 1. init_database.sql

**功能：** 创建数据库和表结构

**创建的表：**

**原始数据表（4个）：**
- `ty_m_unreal_person_call_data_filter` - 用户通话交际圈汇总日表
- `ty_m_unreal_person_user_number_data_filter` - 用户基础信息日表及月表
- `tv_m_cust_single_flux_used` - 单一用户流量信息月表
- `tw_d_is_pack_users_new_used` - 资费套餐订购日明细表

**聚合数据表（3个）：**
- `user_graph` - 用户信息（点）
- `call_graph` - 通话关系（边）
- `tv_analysis` - 用户特征宽表

**使用方法：**
```bash
gsql -d postgres -p 5432 -U username -f init_database.sql
```

### 2. generate_test_data.py

**功能：** 生成模拟测试数据

**配置参数（在文件开头的Config类中）：**
```python
TOTAL_USERS = 10000              # 总用户数
CALLS_PER_USER_PER_DAY = 10      # 每用户每天通话次数
DAYS_TO_GENERATE = 30            # 生成数据天数
NORMAL_USER_RATIO = 0.70         # 正常用户比例
SUSPICIOUS_USER_RATIO = 0.20     # 可疑用户比例
FRAUD_USER_RATIO = 0.10          # 欺诈用户比例
```

**输出文件（在output_data/目录）：**
- `ty_m_unreal_person_user_number_data_filter.csv` - 用户基础信息
- `ty_m_unreal_person_call_data_filter.csv` - 通话记录
- `tv_m_cust_single_flux_used.csv` - 流量信息
- `tw_d_is_pack_users_new_used.csv` - 套餐信息

**使用方法：**
```bash
python3 generate_test_data.py
```

**修改配置：**
直接编辑文件第18-75行的`Config`类即可。

### 3. filter_data.py

**功能：** 将CSV数据转换为TXT格式，用于图分析

**输入：** output_data/*.csv（4个CSV文件）

**输出：** filter_data/*.txt（3个TXT文件）
- `user.txt` - 用户信息（7个字段）
- `call.txt` - 通话关系（4个字段，已去重）
- `tv.txt` - 用户特征宽表（34个字段）

**数据格式（默认与cmcc/data一致）：**
- 分隔符：€€（欧元符号的两个）
- 编码：UTF-8
- 表头：无表头，直接是数据行

**使用方法：**
```bash
python3 filter_data.py
```

**配置修改：**

编辑文件中的`FilterConfig`类可修改输出格式：

```python
class FilterConfig:
    OUTPUT_DELIMITER = '€€'         # 分隔符（默认：€€，可改为'\t'或','）
    OUTPUT_WITH_HEADER = False      # 是否输出表头（默认：False）
```

**可选配置：**
- 生成Tab分隔+表头：`OUTPUT_DELIMITER = '\t'`, `OUTPUT_WITH_HEADER = True`
- 生成CSV格式：`OUTPUT_DELIMITER = ','`, `OUTPUT_WITH_HEADER = True`

## 数据映射关系

### user.txt（用户信息表）

| 目标字段 | 来源表 | 来源字段 | 说明 |
|---------|--------|---------|------|
| MSISDN | 用户基础信息表 | MSISDN | 手机号 |
| USER_ID | 用户基础信息表 | USER_ID | 用户ID |
| NEW_RCN_ID | 用户基础信息表 | 计算 | 根据RCN_DATE计算 |
| RCN_DURA | 用户基础信息表 | 计算 | 入网时长（月） |
| IDTY_NBR | 用户基础信息表 | IDTY_CODE | 身份证号 |
| STATIS_YMD | 用户基础信息表 | STATIS_YMD | 统计日期 |
| PROV_ID | 用户基础信息表 | PROV_ID | 省份编码 |

### call.txt（通话关系表）

| 目标字段 | 来源表 | 来源字段 |
|---------|--------|---------|
| MSISDN | 通话记录表 | MSISDN |
| OPP_MSISDN | 通话记录表 | OPP_MSISDN |
| STATIS_YMD | 通话记录表 | STATIS_YMD |
| PROV_ID | 通话记录表 | PROV_ID |

### tv.txt（用户特征宽表）

包含34个字段，来源于3个表：
- 用户基础信息表（20个字段）
- 流量信息表（9个字段）
- 套餐信息表（4个字段）

详细字段映射请参考`DATA_GENERATION_GUIDE.md`文档。

## 数据规模估算

| 用户数 | 天数 | 通话记录数 | CSV文件大小 | 生成时间 |
|--------|------|-----------|-----------|---------|
| 10,000 | 30 | 3,000,000 | ~500MB | ~30秒 |
| 100,000 | 30 | 30,000,000 | ~5GB | ~5分钟 |
| 1,000,000 | 30 | 300,000,000 | ~50GB | ~50分钟 |

## 常用命令

### 查看生成的数据

```bash
# 查看CSV文件
ls -lh output_data/

# 查看TXT文件
ls -lh filter_data/

# 查看user.txt的前10行
head -n 10 filter_data/user.txt

# 统计各文件行数
wc -l filter_data/*.txt
```

### 清理数据

```bash
# 清理原始CSV数据
rm -rf output_data/

# 清理过滤后的TXT数据
rm -rf filter_data/

# 清理全部生成数据
rm -rf output_data/ filter_data/
```

## 文档清单

| 文档文件 | 内容 |
|---------|------|
| `README.md` | 项目整体说明，Schema定义 |
| `DATA_GENERATION_GUIDE.md` | 详细使用指南 |
| `DEDUPLICATION_MECHANISM.md` | 去重机制技术说明 |
| `SCRIPTS_OVERVIEW.md` | 本文档 - 脚本使用总览 |

## 问题排查

### 问题1：生成数据时提示导入错误

**原因：** Python版本过低

**解决：** 确保Python版本 >= 3.7

```bash
python3 --version
```

### 问题2：filter_data.py提示文件不存在

**原因：** 未先运行generate_test_data.py

**解决：** 先生成原始数据

```bash
python3 generate_test_data.py
python3 filter_data.py
```

### 问题3：想要生成更多用户数据

**解决：** 修改generate_test_data.py中的TOTAL_USERS参数

```python
# 在文件中找到这一行并修改
TOTAL_USERS = 100000  # 改为10万用户
```

### 问题4：想要修改输出分隔符（从Tab改为逗号）

**解决：** 修改filter_data.py中的OUTPUT_DELIMITER参数

```python
# 在文件中找到这一行并修改
OUTPUT_DELIMITER = ','  # 改为逗号分隔
```

## 下一步

数据生成完成后，你可以：

1. **进行图分析**：使用filter_data/目录下的TXT文件
2. **SQL分析**：查询OpenGauss数据库中的数据
3. **业务分析**：根据README.md第2节的业务结果进行特征计算

详细操作请参考`DATA_GENERATION_GUIDE.md`文档。
