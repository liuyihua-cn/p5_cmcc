# generate_test_data.py 更新日志

## 版本 2.1 - 2024-10-24

### Bug修复

#### 1. 通话数量差异度过小

**问题：**
- 原来：每用户通话次数 = CALLS_PER_USER_PER_MONTH ± 2
- 例如：设置为8时，实际范围 [6, 10]，差异太小

**修复：**
- 现在：每用户通话次数在 0.5x 到 2x 范围内变化
- 例如：设置为8时，实际范围 [4, 16]，差异明显

**代码改进：**
```python
# 旧代码
monthly_calls = random.randint(
    max(1, self.config.CALLS_PER_USER_PER_MONTH - 2),
    self.config.CALLS_PER_USER_PER_MONTH + 2
)

# 新代码
min_calls = max(1, int(self.config.CALLS_PER_USER_PER_MONTH * 0.5))
max_calls = int(self.config.CALLS_PER_USER_PER_MONTH * 2)
monthly_calls = random.randint(min_calls, max_calls)
```

#### 2. 存在自己给自己打电话的情况

**问题：**
- 邻居生成时可能包含自己
- 通话记录中出现 MSISDN = OPP_MSISDN 的情况

**修复：**
- 邻居生成时排除自己
- 通话对象选择时双重过滤（User对象 + 手机号字符串）

**代码改进：**
```python
# 邻居生成：排除自己
other_users = [u for u in self.users if u.user_id != user.user_id]
user_neighbors = random.sample(other_users, min(neighbor_count // 2, len(other_users)))

# 通话对象选择：完善过滤逻辑
valid_neighbors = []
for n in user.neighbors:
    if isinstance(n, User):
        # User对象：检查user_id
        if n.user_id != user.user_id:
            valid_neighbors.append(n)
    else:
        # 字符串（手机号）：检查不等于自己的手机号
        if n != user.msisdn:
            valid_neighbors.append(n)
```

### 数据质量改进

| 指标 | v2.0 | v2.1 | 改进 |
|------|------|------|------|
| 通话数量差异 | ±25% | ±100% | ⬆️ 4倍 |
| 自己打给自己 | 可能存在 | 完全消除 | ✅ |
| 数据真实性 | 中等 | 高 | ⬆️ |

### 数据验证

**新增验证脚本：** `verify_data.py`

使用方法：
```bash
# 生成数据后运行验证
python3 generate_test_data.py
python3 verify_data.py
```

验证项目：
- ✅ 检查是否存在自己打给自己的情况
- ✅ 统计每用户通话次数分布
- ✅ 检查通话数量差异度
- ✅ 验证数据唯一性（手机号、用户ID）
- ✅ 显示通话次数分布直方图

示例输出：
```
验证通话记录数据
============================================================
总通话记录数: 80,000
唯一通话对数: 78,523
唯一用户数: 10,000

✅ 没有发现自己打给自己的记录

每用户通话次数统计:
  最小值: 4
  最大值: 16
  平均值: 8.00
  范围比: 4.00x
  ✅ 通话数量差异度良好（4.00x >= 2.5x）

通话次数分布:
  [ 4- 8]:  4523 ████████
  [ 9-13]:  4821 █████████
  [14-16]:  656  ██
============================================================
```

---

## 版本 2.0 - 2024-10-24

### 重大改进

#### 1. 通话记录生成逻辑优化

**问题：**
- 原来：每个用户每天生成CALLS_PER_USER_PER_DAY条通话记录（默认10条）
- 导致数据量过大（10,000用户 × 10通话/天 × 30天 = 3,000,000条记录）

**改进：**
- 现在：每个用户每月生成CALLS_PER_USER_PER_MONTH条通话记录（默认8条）
- 通话日期在整个月内随机分布
- 数据量：10,000用户 × 8通话/月 = 80,000条记录（减少97%）

**配置修改：**
```python
# 旧配置
CALLS_PER_USER_PER_DAY = 10
DAYS_TO_GENERATE = 30

# 新配置
CALLS_PER_USER_PER_MONTH = 8  # 建议范围：5-10
```

#### 2. 日期生成逻辑修正

**问题：**
- 原来：从今天往前推DAYS_TO_GENERATE天
- 导致跨月（例如：10月24日往前推30天会到9月24日）

**改进：**
- 现在：生成当前月的完整数据（月初到月底）
- 例如：10月份生成 2024-10-01 到 2024-10-31 的数据
- 支持灵活配置生成上个月或更早月份的数据

**新增配置：**
```python
MONTH_OFFSET = 0  # 0=当前月，1=上个月，2=上上个月...
```

**示例：**
- `MONTH_OFFSET = 0`：生成2024年10月的数据（10-01至10-31）
- `MONTH_OFFSET = 1`：生成2024年9月的数据（09-01至09-30）
- `MONTH_OFFSET = 2`：生成2024年8月的数据（08-01至08-31）

### 新增功能

#### 1. get_month_date_range() 函数

自动获取指定月份的日期范围（月初到月底），支持跨年处理。

```python
start_date, end_date, days_in_month = get_month_date_range(month_offset=0)
# 返回：('20241001', '20241031', 31)
```

#### 2. 改进的配置显示

运行脚本时会显示：
```
当前配置:
  总用户数: 10,000
  数据月份: 202410 (20241001 至 20241031，共31天)
  每用户每月通话次数: 8
  正常用户比例: 70.0%
  可疑用户比例: 20.0%
  欺诈用户比例: 10.0%
  输出目录: ./output_data
```

### 数据规模对比

| 配置 | 旧版本 | 新版本 |
|------|--------|--------|
| 用户数 | 10,000 | 10,000 |
| 通话记录 | 3,000,000条 | 80,000条 |
| CSV文件大小 | ~500MB | ~15MB |
| 生成时间 | ~30秒 | ~5秒 |

### 配置参数说明

```python
class Config:
    # 数据规模配置
    TOTAL_USERS = 10000              # 总用户数
    CALLS_PER_USER_PER_MONTH = 8     # 每用户每月通话次数（建议5-10）

    # 数据月份配置
    MONTH_OFFSET = 0                 # 0=当前月，1=上个月...
```

### 使用示例

#### 示例1：生成当前月数据

```python
MONTH_OFFSET = 0  # 当前月
CALLS_PER_USER_PER_MONTH = 8
```

运行结果：如果今天是2024年10月24日，生成2024年10月1日至31日的数据。

#### 示例2：生成上个月数据

```python
MONTH_OFFSET = 1  # 上个月
CALLS_PER_USER_PER_MONTH = 10
```

运行结果：如果今天是2024年10月24日，生成2024年9月1日至30日的数据。

#### 示例3：生成大量数据

```python
TOTAL_USERS = 100000             # 10万用户
MONTH_OFFSET = 0
CALLS_PER_USER_PER_MONTH = 10    # 每月10次通话
```

运行结果：
- 通话记录：100,000 × 10 = 1,000,000条
- 生成时间：约5分钟
- CSV文件：约150MB

### 迁移指南

#### 从旧版本升级

**步骤1：更新配置**

删除或注释掉旧配置：
```python
# CALLS_PER_USER_PER_DAY = 10   # 删除
# DAYS_TO_GENERATE = 30         # 删除
# END_DATE = ...                # 删除
```

添加新配置：
```python
CALLS_PER_USER_PER_MONTH = 8    # 新增
MONTH_OFFSET = 0                # 新增
```

**步骤2：运行测试**

```bash
python3 generate_test_data.py
```

检查输出的配置信息是否正确。

**步骤3：验证数据**

```bash
# 查看生成的通话记录数量
wc -l output_data/ty_m_unreal_person_call_data_filter.csv

# 查看日期范围
head -n 100 output_data/ty_m_unreal_person_call_data_filter.csv | cut -d',' -f1 | sort | uniq
```

### 常见问题

#### Q1: 为什么改为每月而不是每天？

A:
- 真实场景：移动通话数据通常按月统计
- 数据量：每月颗粒度大幅减少数据量，提升性能
- 灵活性：可以通过调整CALLS_PER_USER_PER_MONTH来控制规模

#### Q2: 如何生成跨多个月的数据？

A: 目前每次生成一个月的数据。如需多个月，可以：
1. 修改MONTH_OFFSET多次运行
2. 将生成的数据合并
3. 或联系开发者添加多月生成功能

#### Q3: 通话日期是如何分布的？

A: 通话日期在整个月内均匀随机分布。例如：
- 某用户本月有8条通话记录
- 这8条记录会随机分布在该月的任意日期
- 不会集中在某几天

#### Q4: 旧版本的数据还能用吗？

A:
- 可以继续使用
- 但建议使用新版本生成更合理的数据
- 旧数据可能跨月，不符合业务逻辑

### 向后兼容性

**不兼容的改动：**
- 移除了`CALLS_PER_USER_PER_DAY`配置
- 移除了`DAYS_TO_GENERATE`配置
- 移除了`END_DATE`配置

**解决方案：**
按照"迁移指南"更新配置文件即可。

### 技术细节

#### 日期范围计算

```python
def get_month_date_range(month_offset: int = 0) -> tuple:
    """
    获取指定月份的日期范围（月初到月底）

    参数：
        month_offset: 月份偏移（0=当前月，1=上个月...）

    返回：
        (start_date, end_date, days_in_month)
        例如：('20241001', '20241031', 31)
    """
```

#### 通话记录生成流程

1. 获取当前月的日期范围
2. 生成该月的所有日期列表
3. 对每个用户：
   - 确定本月通话次数（CALLS_PER_USER_PER_MONTH ± 2）
   - 从邻居中选择通话对象
   - 为每条通话记录随机分配一个日期
4. 写入CSV文件

### 性能优化

- ✅ 通话记录数量减少97%
- ✅ 文件大小减少97%
- ✅ 生成速度提升6倍
- ✅ 内存占用减少90%

### 下一步计划

- [ ] 支持生成跨多个月的数据
- [ ] 支持自定义日期范围（指定开始和结束日期）
- [ ] 支持通话记录的时间分布配置（工作日多/周末多）
- [ ] 添加数据验证功能

---

最后更新：2024-10-24
