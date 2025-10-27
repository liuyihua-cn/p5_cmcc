# filter_data.py 配置示例

本文档提供不同场景下的配置示例。

## 默认配置（与cmcc/data格式一致）

**用途：** 生成与cmcc/data目录相同格式的数据

**配置：**
```python
class FilterConfig:
    OUTPUT_DELIMITER = '€€'         # 欧元符号分隔
    OUTPUT_WITH_HEADER = False      # 不输出表头
```

**输出示例：**
```
ecada95de963d211cccb523efb728969€€21d6a36957ef55162b68fc93c2714ddc€€1€€11€€37693cfc748049e45d87b8c7d8b9aacd€€20220718€€10000
df14653939d07a00159b2f756cb63799€€9662acfbd200cd57f95c5a753829cb90€€0€€59€€8f14e45fceea167a5a36dedd4bea2543€€20220718€€10000
```

**使用场景：**
- ✅ 用于图计算系统
- ✅ 与cmcc/data格式保持一致
- ✅ 适合大数据处理

---

## 配置1：Tab分隔 + 无表头

**用途：** TSV格式，无表头

**配置：**
```python
class FilterConfig:
    OUTPUT_DELIMITER = '\t'         # Tab分隔
    OUTPUT_WITH_HEADER = False      # 不输出表头
```

**输出示例：**
```
ecada95de963d211cccb523efb728969	21d6a36957ef55162b68fc93c2714ddc	1	11	37693cfc748049e45d87b8c7d8b9aacd	20220718	10000
df14653939d07a00159b2f756cb63799	9662acfbd200cd57f95c5a753829cb90	0	59	8f14e45fceea167a5a36dedd4bea2543	20220718	10000
```

**使用场景：**
- ✅ 导入到数据库（COPY命令）
- ✅ 数据ETL流程
- ✅ 与某些数据工具集成

---

## 配置2：Tab分隔 + 有表头

**用途：** TSV格式，带表头

**配置：**
```python
class FilterConfig:
    OUTPUT_DELIMITER = '\t'         # Tab分隔
    OUTPUT_WITH_HEADER = True       # 输出表头
```

**输出示例：**
```
MSISDN	USER_ID	NEW_RCN_ID	RCN_DURA	IDTY_NBR	STATIS_YMD	PROV_ID
ecada95de963d211cccb523efb728969	21d6a36957ef55162b68fc93c2714ddc	1	11	37693cfc748049e45d87b8c7d8b9aacd	20220718	10000
df14653939d07a00159b2f756cb63799	9662acfbd200cd57f95c5a753829cb90	0	59	8f14e45fceea167a5a36dedd4bea2543	20220718	10000
```

**使用场景：**
- ✅ Excel/Numbers打开查看
- ✅ Pandas读取（`pd.read_csv(..., sep='\t')`）
- ✅ 数据探索和验证

---

## 配置3：CSV格式 + 有表头

**用途：** 标准CSV格式

**配置：**
```python
class FilterConfig:
    OUTPUT_DELIMITER = ','          # 逗号分隔
    OUTPUT_WITH_HEADER = True       # 输出表头
```

**输出示例：**
```
MSISDN,USER_ID,NEW_RCN_ID,RCN_DURA,IDTY_NBR,STATIS_YMD,PROV_ID
ecada95de963d211cccb523efb728969,21d6a36957ef55162b68fc93c2714ddc,1,11,37693cfc748049e45d87b8c7d8b9aacd,20220718,10000
df14653939d07a00159b2f756cb63799,9662acfbd200cd57f95c5a753829cb90,0,59,8f14e45fceea167a5a36dedd4bea2543,20220718,10000
```

**使用场景：**
- ✅ Excel/Google Sheets打开
- ✅ 通用数据交换格式
- ✅ 大多数数据分析工具支持

---

## 配置4：竖线分隔 + 无表头

**用途：** 使用竖线作为分隔符

**配置：**
```python
class FilterConfig:
    OUTPUT_DELIMITER = '|'          # 竖线分隔
    OUTPUT_WITH_HEADER = False      # 不输出表头
```

**输出示例：**
```
ecada95de963d211cccb523efb728969|21d6a36957ef55162b68fc93c2714ddc|1|11|37693cfc748049e45d87b8c7d8b9aacd|20220718|10000
df14653939d07a00159b2f756cb63799|9662acfbd200cd57f95c5a753829cb90|0|59|8f14e45fceea167a5a36dedd4bea2543|20220718|10000
```

**使用场景：**
- ✅ 与某些传统数据库系统集成
- ✅ 数据仓库导入

---

## 如何修改配置

### 方法1：直接编辑文件

打开`filter_data.py`文件，找到第18行开始的`FilterConfig`类：

```python
class FilterConfig:
    """过滤配置"""
    INPUT_DIR = "./output_data"
    OUTPUT_DIR = "./filter_data"

    # ... 其他配置 ...

    # 输出格式配置（修改这里）
    OUTPUT_DELIMITER = '€€'         # 👈 修改分隔符
    OUTPUT_WITH_HEADER = False      # 👈 修改是否输出表头
```

### 方法2：使用不同配置文件

可以复制脚本创建不同版本：

```bash
# 复制脚本
cp filter_data.py filter_data_csv.py
cp filter_data.py filter_data_tsv.py

# 分别修改配置，用于不同场景
# filter_data.py      - 默认€€格式
# filter_data_csv.py  - CSV格式
# filter_data_tsv.py  - TSV格式
```

---

## 常见问题

### Q1: 如何在Excel中打开€€分隔的文件？

**A:** 不推荐在Excel中打开€€格式。如需Excel查看，使用配置2或配置3。

### Q2: Pandas如何读取不同格式？

**A:** 根据格式使用不同参数：

```python
import pandas as pd

# €€格式（无表头）
df = pd.read_csv('user.txt', sep='€€', header=None,
                 names=['MSISDN', 'USER_ID', 'NEW_RCN_ID', 'RCN_DURA', 'IDTY_NBR', 'STATIS_YMD', 'PROV_ID'])

# Tab格式（有表头）
df = pd.read_csv('user.txt', sep='\t')

# CSV格式（有表头）
df = pd.read_csv('user.txt')
```

### Q3: 如何导入到PostgreSQL/OpenGauss？

**A:** 推荐使用配置1（Tab + 无表头）：

```sql
COPY table_name FROM '/path/to/user.txt' WITH (DELIMITER E'\t', HEADER false);
```

### Q4: 分隔符可以是多字符吗？

**A:** 可以，但注意：
- ✅ €€（两个字符）- 支持
- ✅ ::（两个字符）- 支持
- ❌ 某些工具可能不支持多字符分隔符

### Q5: 如何验证生成的文件格式正确？

**A:** 使用以下命令：

```bash
# 查看前几行
head -n 3 filter_data/user.txt

# 查看分隔符（€€会显示为特殊字符）
cat filter_data/user.txt | head -n 1 | od -c

# 统计字段数（€€分隔符）
head -n 1 filter_data/user.txt | awk -F'€€' '{print NF}'
# 应该输出：7（user.txt有7个字段）
```

---

## 推荐配置

| 使用场景 | 推荐配置 | 分隔符 | 表头 |
|---------|---------|--------|------|
| 图计算系统 | 默认配置 | €€ | 无 |
| 数据库导入 | 配置1 | Tab | 无 |
| Excel查看 | 配置2/3 | Tab/逗号 | 有 |
| Pandas分析 | 配置2 | Tab | 有 |
| 数据交换 | 配置3 | 逗号 | 有 |

---

## 快速切换配置

如果需要频繁切换配置，可以使用命令行参数（需要修改脚本添加参数支持）或环境变量。

**简单方案：创建多个配置版本**

```bash
# 创建配置备份
cp filter_data.py filter_data.py.bak

# 需要CSV格式时
# 手动修改 OUTPUT_DELIMITER = ','
# 手动修改 OUTPUT_WITH_HEADER = True
python3 filter_data.py

# 恢复默认配置
cp filter_data.py.bak filter_data.py
```

---

## 总结

默认配置（€€分隔，无表头）与cmcc/data保持一致，适合图计算系统。如需其他格式，参考本文档修改配置即可。
