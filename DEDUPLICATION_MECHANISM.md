# 数据生成去重机制说明

## 问题背景

在生成大规模测试数据时（比如100万用户），使用纯随机方式可能导致以下问题：

1. **手机号重复**：虽然理论上有63亿种可能（7×9亿），但随机生成仍可能碰撞
2. **用户ID重复**：UUID碰撞概率极低但仍需防范
3. **身份证号重复**：原始实现使用无效日期格式，容易产生重复

## 解决方案

### 1. 改进的生成函数

所有关键字段的生成函数都添加了去重参数：

```python
def generate_phone_number(existing_phones: Set[str] = None) -> str
def generate_user_id(existing_ids: Set[str] = None) -> str
def generate_id_card(existing_ids: Set[str] = None) -> str
```

**工作原理：**
- 接受一个`Set`类型的参数，存储已生成的值
- 生成新值时，检查是否已存在
- 如果存在，重新生成（最多尝试1000次）
- 如果1000次仍然失败，使用UUID保证唯一性

### 2. DataGenerator类的去重集合

在`DataGenerator`类中维护三个去重集合：

```python
self.existing_phones: Set[str] = set()      # 已生成的手机号
self.existing_user_ids: Set[str] = set()    # 已生成的用户ID
self.existing_id_cards: Set[str] = set()    # 已生成的身份证号
```

这些集合在整个数据生成过程中持续使用，确保全局唯一性。

### 3. 身份证生成逻辑改进

**原始版本问题：**
```python
# 错误：生成的不是有效日期
id_card = f"{random.randint(100000, 999999)}{random.randint(19500101, 20051231)}{random.randint(1000, 9999)}"
```

**改进版本：**
```python
# 地区码（6位）
region_code = random.randint(100000, 659001)

# 出生日期（8位，使用真实日期）
birth_year = random.randint(1950, 2005)
birth_month = random.randint(1, 12)

# 根据月份确定天数
if birth_month in [1, 3, 5, 7, 8, 10, 12]:
    max_day = 31
elif birth_month in [4, 6, 9, 11]:
    max_day = 30
else:  # 2月
    max_day = 28

birth_day = random.randint(1, max_day)
birth_date = f"{birth_year:04d}{birth_month:02d}{birth_day:02d}"

# 顺序码（3位）+ 校验码（1位）
sequence = random.randint(100, 999)
check_digit = random.randint(0, 9)

id_card = f"{region_code}{birth_date}{sequence}{check_digit}"
```

## 性能影响

### 内存消耗

去重集合的内存消耗：

| 用户数量 | 手机号集合 | 用户ID集合 | 身份证集合 | 总计（约） |
|---------|-----------|-----------|-----------|-----------|
| 10,000 | 0.3 MB | 0.3 MB | 0.1 MB | 0.7 MB |
| 100,000 | 3 MB | 3 MB | 1 MB | 7 MB |
| 1,000,000 | 30 MB | 30 MB | 10 MB | 70 MB |

**结论：** 即使生成100万用户，去重集合也只占用约70MB内存，完全可以接受。

### 时间复杂度

- **查找操作：** O(1) - Set的查找是常数时间
- **插入操作：** O(1) - Set的插入是常数时间
- **碰撞重试：** 最多1000次，对于大多数情况，第一次就成功

**结论：** 去重机制对性能影响微乎其微。

## 碰撞概率分析

### 手机号

**理论空间：** 7 × 900,000,000 = 6,300,000,000

**生日悖论估算：**
- 10,000用户：碰撞概率 ≈ 0.0008% （几乎为0）
- 100,000用户：碰撞概率 ≈ 0.08%
- 1,000,000用户：碰撞概率 ≈ 8%

有了去重机制，即使理论上有8%碰撞概率，也会自动重新生成。

### 用户ID (UUID4)

**理论空间：** 2^122 ≈ 5.3 × 10^36

**碰撞概率：** 即使生成10亿个UUID，碰撞概率也小于10^-18，可以忽略不计。

### 身份证

**改进后的理论空间：**
- 地区码：559,001种
- 出生日期：约20,000种（56年×365天）
- 顺序码+校验：9,000种
- **总计：** 559,001 × 20,000 × 9,000 ≈ 1.0 × 10^14

碰撞概率极低，但去重机制提供额外保障。

## 降级策略

如果随机生成1000次仍然失败（理论上几乎不可能），使用UUID作为后备：

```python
# 手机号降级
return generate_md5(f"PHONE_{uuid.uuid4()}")

# 用户ID降级
return generate_md5(f"USER_{uuid.uuid4()}_{random.randint(0, 999999)}")

# 身份证降级
return generate_md5(f"ID_{uuid.uuid4()}")
```

这确保即使在极端情况下也能保证唯一性。

## 使用示例

### 生成10万用户

```bash
# 修改配置
# 在 generate_test_data.py 中：
TOTAL_USERS = 100000

# 运行脚本
python3 generate_test_data.py
```

**预期结果：**
- 生成时间：约5分钟
- 内存占用：约500MB（包括去重集合7MB）
- 保证：100%无重复

### 生成100万用户

```bash
# 修改配置
TOTAL_USERS = 1000000

# 运行脚本
python3 generate_test_data.py
```

**预期结果：**
- 生成时间：约50分钟
- 内存占用：约5GB（包括去重集合70MB）
- 保证：100%无重复

## 验证去重效果

在数据导入后，可以运行以下SQL验证无重复：

```sql
-- 检查手机号是否重复
SELECT MSISDN, COUNT(*) as cnt
FROM ty_m_unreal_person_user_number_data_filter
GROUP BY MSISDN
HAVING COUNT(*) > 1;
-- 应返回0行

-- 检查用户ID是否重复
SELECT USER_ID, COUNT(*) as cnt
FROM ty_m_unreal_person_user_number_data_filter
GROUP BY USER_ID
HAVING COUNT(*) > 1;
-- 应返回0行

-- 检查手机号在通话记录中的唯一性
SELECT COUNT(DISTINCT MSISDN) as unique_msisdn,
       COUNT(DISTINCT OPP_MSISDN) as unique_opp_msisdn
FROM ty_m_unreal_person_call_data_filter;
```

## 总结

✅ **问题已解决：** 添加了完善的去重机制

✅ **性能优化：** 使用Set数据结构，O(1)时间复杂度

✅ **内存可控：** 100万用户仅需70MB去重集合

✅ **降级保护：** UUID作为最后的保障

✅ **真实数据：** 身份证日期格式修正为有效日期

现在即使生成百万级用户数据，也能保证所有关键字段的唯一性！
