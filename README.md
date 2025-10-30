# 移动反欺诈场景测试方案

# 1. Schema定义

在该反欺诈业务场景中，Schema可以分为两层，一个是原始的数据表，另一个是从原始数据表中抽出来的关键属性。前者是数据的最初形态，后者是聚合产生的分析要用的数据。

具体地，元素数据表共有4个如下所示。

- 用户通话交际圈汇总日表：ty_m_unreal_person_call_data_filter
- 用户基础信息日表及月表：ty_m_unreal_person_user_number_data_filter
- 单一用户流量信息月表：tv_m_cust_single_flux_used
- 资费套餐订购日明细表：tw_d_is_pack_users_new_used

而最终聚合产生的数据表共有3个如下所示。

- call：通话关系，用于构建边，包含4个字段
- tv：宽表，用来分析用户特征变化，包含34个字段
- user：用户信息，用于构建点，包含7个字段

## 1.1 原始数据表

原始数据表的定义基本在《基于图计算的实名不实人检测需求说明书》中有着较为清晰的定义。不过，在实际上数据的最终字段和说明书上有些许差别，下面将表字段的详细定义列举如下。

### 1.1.1 用户通话交际圈汇总日表

该表在《基于图计算的实名不实人检测需求说明书》中定义，包含较多字段，但是只有4个字段会被使用生成call表，并且实际上只有2个字段被真实使用。

| **字段名称**         | **字段含义**           | **类型**       | **备注**         |
| -------------------- | ---------------------- | -------------- | ---------------- |
| 统计日期（年月日）   | STATIS_YMD             | STRING         | call             |
| 移动号码             | MSISDN                 | STRING         | call（真实使用） |
| 省份标识             | PROV_ID                | STRING         | call             |
| 呼叫类型编码         | CALL_TYP_CODE          | STRING         |                  |
| 对方号码             | OPP_MSISDN             | STRING         | call（真实使用） |
| 对端归属地市         | OPP_BELO_CITY          | STRING         |                  |
| 对端运营商品牌编码   | OPP_ISP_BRAND_CODE     | STRING         |                  |
| 通话时长(秒)         | CALL_DURA_SEC          | DECIMAL(18)    |                  |
| 6点通话时长(秒)      | H6_CALL_DURA_SEC       | DECIMAL(18)    |                  |
| 7点通话时长(秒)      | H7_CALL_DURA_SEC       | DECIMAL(18)    |                  |
| 8点通话时长(秒)      | H8_CALL_DURA_SEC       | DECIMAL(18)    |                  |
| 9点通话时长(秒)      | H9_CALL_DURA_SEC       | DECIMAL(18)    |                  |
| 10点通话时长(秒)     | H10_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 11点通话时长(秒)     | H11_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 12点通话时长(秒)     | H12_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 13点通话时长(秒)     | H13_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 14点通话时长(秒)     | H14_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 15点通话时长(秒)     | H15_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 16点通话时长(秒)     | H16_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 17点通话时长(秒)     | H17_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 18点通话时长(秒)     | H18_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 19点通话时长(秒)     | H19_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 20点通话时长(秒)     | H20_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 21点通话时长(秒)     | H21_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 22点通话时长(秒)     | H22_CALL_DURA_SEC      | DECIMAL(18)    |                  |
| 23点-6点通话时长(秒) | H23_H6_CALL_DURA_SEC   | DECIMAL(18)    |                  |
| 计费时长（分钟）     | BILL_DURA_MINU         | DECIMAL(18，4) |                  |
| 长途计费时长（秒）   | TOLL_BILL_DURA_SEC     | DECIMAL(18)    |                  |
| 优惠后通话费         | FAV_FLW_CALL_INC_YUAN  | DECIMAL(18，4) |                  |
| 折扣后基本费         | FAV_FLW_CFEE_YUAN      | DECIMAL(18，4) |                  |
| 折扣后长途费用       | FAV_FLW_TOLL_FEE_YUAN  | DECIMAL(18，4) |                  |
| 优惠后其他费         | FAV_FLW_OTHER_FEE_YUAN | DECIMAL(18，4) |                  |
| 通话次数             | CALL_CNT               | DECIMAL(18)    |                  |
| 6点通话次数          | H6_CALL_CNT            | DECIMAL(18)    |                  |
| 7点通话次数          | H7_CALL_CNT            | DECIMAL(18)    |                  |
| 8点通话次数          | H8_CALL_CNT            | DECIMAL(18)    |                  |
| 9点通话次数          | H9_CALL_CNT            | DECIMAL(18)    |                  |
| 10点通话次数         | H10_CALL_CNT           | DECIMAL(18)    |                  |
| 11点通话次数         | H11_CALL_CNT           | DECIMAL(18)    |                  |
| 12点通话次数         | H12_CALL_CNT           | DECIMAL(18)    |                  |
| 13点通话次数         | H13_CALL_CNT           | DECIMAL(18)    |                  |
| 14点通话次数         | H14_CALL_CNT           | DECIMAL(18)    |                  |
| 15点通话次数         | H15_CALL_CNT           | DECIMAL(18)    |                  |
| 16点通话次数         | H16_CALL_CNT           | DECIMAL(18)    |                  |
| 17点通话次数         | H17_CALL_CNT           | DECIMAL(18)    |                  |
| 18点通话次数         | H18_CALL_CNT           | DECIMAL(18)    |                  |
| 19点通话次数         | H19_CALL_CNT           | DECIMAL(18)    |                  |
| 20点通话次数         | H20_CALL_CNT           | DECIMAL(18)    |                  |
| 21点通话次数         | H21_CALL_CNT           | DECIMAL(18)    |                  |
| 22点通话次数         | H22_CALL_CNT           | DECIMAL(18)    |                  |
| 23点-6点通话次数     | H23_H6_CALL_CNT        | DECIMAL(18)    |                  |
| 话单来源             | CDR_SRC                | STRING         |                  |

### 1.1.2 用户基础信息日表及月表

用于产生user和tv表。

| **字段名称**               | **字段含义**           | **类型** | **备注**                                              |
| -------------------------- | ---------------------- | -------- | ----------------------------------------------------- |
| STATIS_YMD                 | 统计日期（年月日）     | string   | user待定                                              |
| PROV_ID                    | 省份标识               | string   | user待定                                              |
| CITY_CODE                  | 地市编码               | string   |                                                       |
| USER_ID                    | 用户标识               | string   | tv和user                                              |
| CUST_ID                    | 客户标识               | string   |                                                       |
| NBR_PAR                    | 号段                   | string   |                                                       |
| IS_PRETTY_NUM              | 是否靓号               | string   | tv                                                    |
| PRETTY_NUM_TYP             | 靓号类型               | string   | tv                                                    |
| VIP_CUST_ID                | 重要客户标识           | string   | tv                                                    |
| VIP_LVL                    | 会员级别               | string   | tv                                                    |
| INDV_CUST_LVL_CODE         | 个人客户级别编码       | string   |                                                       |
| AGE                        | 年龄                   | string   |                                                       |
| AGE_LVL                    | 年龄分档               | string   | tv                                                    |
| SEX                        | 性别                   | string   | tv                                                    |
| IDTY_TYP                   | 证件类型               | string   |                                                       |
| IDTY_CODE                  | 证件编号               | string   | 用于产生user的IDTY_NBR                                |
| IDCARD_BIRTH_AREA          | 身份证出生区域         | string   |                                                       |
| IDCARD_BIRTH_DATE          | 身份证出生日期         | string   |                                                       |
| SAME_IDCARD_MSISDN_CNT     | 同一身份证移动号码数   | int      |                                                       |
| NANL                       | 民族                   | string   |                                                       |
| OCPN_CODE                  | 职业编码               | string   | tv                                                    |
| EDUCAT_DEGREE_CODE         | 教育程度编码           | string   | tv                                                    |
| USER_TYP                   | 用户类型               | string   |                                                       |
| BUSI_TYP                   | 业务类型               | string   |                                                       |
| PAY_TYP                    | 付费类型               | string   |                                                       |
| BRAND_ID                   | 品牌标识               | string   | tv                                                    |
| DATA_SIM_AND_M2M_USER_FLAG | 数据SIM卡及M2M用户标志 | string   |                                                       |
| IS_M2M_ID_TD_SPECI         | 是否物联网标识(TD口径) | string   |                                                       |
| MSISDN                     | 移动号码               | string   | tv和user                                              |
| IMSI                       | IMSI号                 | string   |                                                       |
| IS_RNAME                   | 是否实名               | string   |                                                       |
| RCN_DATE                   | 入网日期               | string   | tv                                                     |
| NEW_RCN_ID                 | 是否新入网用户         | string   | user（1表示是新入网用户，0表示非新入网用户）          |
| RCN_DURA                   | 入网时长（月）         | int      | user                                                   |
| RCN_CHNL_ID                | 入网渠道标识           | string   |                                                       |
| RCN_CHNL_TYP               | 入网渠道类型           | string   | tv                                                    |
| RCN_MODE                   | 入网方式               | string   |                                                       |
| IS_CAMP_USER               | 是否校园用户           | string   | tv                                                    |
| IS_CAMP_AREA_USER          | 是否校园区域用户       | string   | tv                                                    |
| BELO_CAMP_ID               | 所属校园标识           | string   |                                                       |
| IS_GROUP_USER              | 是否集团用户           | string   | tv                                                    |
| MEMB_TYP                   | 成员类型               | string   | tv                                                    |
| IS_GROUP_KEY_INDV          | 是否集团关键人         | string   | tv                                                    |
| BECOME_GROUP_USER_MEMB_TM  | 成为集团用户成员时间   | string   |                                                       |
| GROUP_INDUS_TYP_CODE       | 集团行业分类编码       | string   |                                                       |
| GROUP_USER_UNIPAY_FLAG     | 集团用户统一付费标志   | string   |                                                       |
| BELO_GROUP_CUST_ID         | 归属集团客户标识       | string   |                                                       |
| IS_GSM_USER                | 是否是全球通用户       | string   | tv                                                    |
| GSM_USER_LVL               | 全球通用户级别         | string   | tv                                                    |
| GSM_USER_SRC               | 全球通用户来源         | string   |                                                       |
| USER_STATUS                | 用户状态               | string   |                                                       |
| STATUS_CHNG_DATE           | 状态变更日期           | string   |                                                       |
| IS_THIS_INNET              | 是否本期在网           | string   |                                                       |
| INNET_DURA                 | 在网时长               | int      |                                                       |
| INNET_DURA_LVL_CODE        | 在网时长层次编码       | string   | tv                                                    |
| IS_CM_NADD                 | 是否本月新增           | string   |                                                       |
| IS_THIS_EXIT               | 是否本期离网           | string   |                                                       |
| IS_FUSE_BRD                | 是否融合宽带           | string   |                                                       |
| IS_THIS_STP                | 是否本期停机           | string   |                                                       |
| LAST_ONE_STP_TM            | 最后一次停机时间       | string   |                                                       |
| STP_DURA                   | 停机时长               | int      |                                                       |
| STP_TYP                    | 停机类型               | string   |                                                       |
| CANCL_DATE                 | 销帐日期               | string   |                                                       |
| MSISDN_OWE_STP_FREQ        | 手机号码欠费停机频次   | int      |                                                       |
| EXIT_TYP                   | 离网类型               | string   |                                                       |
| YEAR_NADD_ID               | 年新增标识             | string   |                                                       |
| IS_ENJOY_GSM               | 是否尊享全球通         | string   |                                                       |
| UNIQUE_FLAG                | 唯一性标记             | string   |                                                       |
| USER_AREA_BELO             | 用户区域归属           | string   | tv                                                    |
| IS_GIVE_CARD               | 是否赠送卡片           | string   |                                                       |
| PRETTY_NUM_TYP_NAME        | 靓号类型名称           | string   |                                                       |
| IS_CM_NADD_GIVE_CARD       | 是否本月新增赠送卡片   | string   |                                                       |
| IS_ASS_CARD                | 是否副卡               | string   |                                                       |
| CUST_NAME                  | 客户姓名               | string   |                                                       |
| CNTY_CODE                  | 区县编码               | string   |                                                       |
| MAGE_STATUS                | 婚姻状况               | string   |                                                       |
| NATION                     | 国籍                   | string   |                                                       |
| BIND_BRD_CNT               | 绑定宽带笔数           | int      |                                                       |

### 1.1.3 单一用户流量信息月表

用于产生tv表。另外，在实际上字段定义上，代码里面基本都是string，这里和文档里面稍微有些区别，待后续继续确认。

| 字段名称                    | 字段含义                  | 类型    |                        |
| --------------------------- | ------------------------- | ------- | ---------------------- |
| MSISDN                      | 移动号码                  | string  |                        |
| ONNET_FLUX_INCL_VOLTE       | 上网流量包含VOLTE         | decimal |                        |
| ONNET_ALL_FLUX              | 上网全部流量              | decimal | tv                     |
| WDAY_ONNET_FLUX_INCL_VOLTE  | 工作日上网流量包含VOLTE   | decimal |                        |
| WDAY_ONNET_FLUX             | 工作日上网流量            | decimal | tv                     |
| WDAY_H1_ONNET_FLUX          | 工作日1点上网流量         | decimal |                        |
| ONNET_FLUX_4G               | 上网流量4G                | decimal | tv                     |
| GPRS_ACES_DURA              | GPRS连接时间长            | decimal |                        |
| NWDAY_ONNET_FLUX_INCL_VOLTE | 非工作日上网流量(含VOLTE) | decimal |                        |
| NWDAY_ONNET_FLUX            | 非工作日上网流量          | decimal | tv                     |
| FLUX_VOLTE                  | 流量VOLTE                 | decimal |                        |
| UP_FLUX_VOLTE               | 上行流量VOLTE             | decimal |                        |
| FLUX_M2M                    | 流量物联网                | decimal |                        |
| OTHER_NTW_TYP_FLUX          | 其他网络类型流量          | decimal |                        |
| CMWAP_FLUX                  | CMWAP流量                 | decimal |                        |
| CMNET_FLUX                  | CMNET流量                 | decimal |                        |
| LOC_ONNET_FLUX              | 本地上网流量              | decimal |                        |
| ROAM_ONNET_FLUX             | 漫游上网流量              | decimal |                        |
| GPRS_ACES_DURA              | GPRS连接时间长            | decimal |                        |
| FLUX_DURA_M2M               | 流量时长物联网            | decimal |                        |
| LOC_ONNET_DURA              | 本地上网时长              | decimal |                        |
| ROAM_ONNET_DURA             | 漫游上网时长              | decimal |                        |
| ONNET_CNT                   | 上网次数                  | decimal |                        |
| LOC_ONNET_CNT               | 本地上网次数              | decimal |                        |
| ROAM_ONNET_CNT              | 漫游上网次数              | decimal |                        |
| ONNET_DAYS                  | 上网天数                  | decimal |                        |
| ROAM_FLUX_DAYS              | 漫游流量天数              | decimal |                        |
| ONNET_FLUX_3G               | 上网流量3G                | decimal | 原始文档未提及，用于tv |
| TOT_FLUX_5G                 | 总流量5G                  | decimal | 原始文档未提及，用于tv |
| FLUX_FE                     | 流量费用                  | decimal | 原始文档未提及，用于tv |
| FLUX_FEE_4G                 | 流量费用4G                | decimal | 原始文档未提及，用于tv |
| FLUX_TOT_FEE_5G             | 流量总费用5G              | decimal | 原始文档未提及，用于tv |

### 1.1.4 单一用户流量信息月表

用于产生tv表。

| **字段名称**              | **字段含义**         | **类型** | **使用描述**           |
| ------------------------- | -------------------- | -------- | ---------------------- |
| USER_ID                   | 用户标识             | string   |                        |
| MSISDN                    | 用户号码             | string   |                        |
| CITY_CODE                 | 地市                 | string   |                        |
| CHARGE_PACKAGE_UNIFY_CODE | 资费套餐统一编码     | string   | tv                     |
| PACKAGE_HADL_TM           | 套餐办理时间         | string   |                        |
| ORD_STATUS_TYP_CODE       | 资费订购状态         | string   |                        |
| CHARGE_PACKAGE_TYP        | 资费套餐类型         | string   | tv                     |
| PACK_FEE                  | 资费套餐月费         | string   |                        |
| HADL_CHNL_ID              | 办理渠道全网统一编码 | string   |                        |
| IS_CUR_HADL               | 是否当日办理         | string   |                        |
| IS_USE                    | 是否使用             | string   |                        |
| IS_ARRIVE                 | 是否到达             | string   |                        |
| IS_NOLIMIT                | 是否不限量           | string   |                        |
| IS_RENT_CARD              | 是否日租卡           | string   |                        |
| IS_FLY_ENJOY              | 是否飞享             | string   |                        |
| IS_REACH_AMT_NO_SPEED     | 是否达量不限速       | string   |                        |
| IS_FAMPACK                | 是否全家享套餐用户   | string   |                        |
| IS_FAMGIFT                | 是否全家享礼包用户   | string   |                        |
| IS_5G_PACKAGE             | 是否5G套餐           | string   |                        |
| PACKAGE_5G_ID             | 5g套餐类型           | string   | tv                     |
| STATIS_YMD                | 统计日（年月日）     | string   |                        |
| PROV_ID                   | 省份标识             | string   |                        |
| PACK_MON                  | 套餐月费             | string   | 原始文档未提及，用于tv |

## 1.2 聚合数据表

### 1.2.1 user表

user表用于作为点存在，其结构如下所示。

| **字段名称** | **字段含义**   | **类型** | **样例**                         |
| ------------ | -------------- | -------- | -------------------------------- |
| MSISDN       | 手机号码       | string   | ecada95de963d211cccb523efb728969 |
| USER_ID      | 用户唯一标识   | string   | 21d6a36957ef55162b68fc93c2714ddc |
| NEW_RCN_ID   | 是否新入网用户 | string   | 1                                |
| RCN_DURA     | 入网时长（月） | string   | 11                               |
| IDTY_NBR     | 身份证号       | string   | 37693cfc748049e45d87b8c7d8b9aacd |
| STATIS_YMD   | 统计日期       | string   | 20220718                         |
| PROV_ID      | 省份编码       | string   | 10000                            |

### 1.2.2 cal表

call表用于作为边存在，其结构如下所示。

| **字段名称** | **字段含义** | **类型** | **样例**                         |
| ------------ | ------------ | -------- | -------------------------------- |
| MSISDN       | 主叫手机号   | string   | 2b29f13d756036e94e8d1942f36ad9e6 |
| OPP_MSISDN   | 被叫手机号   | string   | 53c28528e5781267f8ad404a35e90401 |
| STATIS_YMD   | 通话日期     | string   | 20220718                         |
| PROV_ID      | 省份编码     | string   | 10000                            |

### 1.2.3 tv表

tv表不参与图结构，而是用于其他业务分析。

| **字段名称**              | **字段含义**        | **数据类型** | **示例值**                       |
| ------------------------- | ------------------- | ------------ | -------------------------------- |
| USER_ID                   | 用户唯一标识        | string       | 21d6a36957ef55162b68fc93c2714ddc |
| MSISDN                    | 手机号码            | string       | ecada95de963d211cccb523efb728969 |
| IS_PRETTY_NUM             | 是否靓号            | string       | 19                               |
| PRETTY_NUM_TYP            | 靓号类型            | string       | 75                               |
| VIP_CUST_ID               | 重要客户标识        | string       | 79                               |
| VIP_LVL                   | 会员级别            | string       | 65                               |
| AGE_LVL                   | 年龄分档            | string       | 79                               |
| SEX                       | 性别                | string       | 22                               |
| OCPN_CODE                 | 职业编码            | string       | 44                               |
| EDUCAT_DEGREE_CODE        | 教育程度编码        | string       | 26                               |
| BRAND_ID                  | 品牌标识            | string       | 63                               |
| RCN_CHNL_TYP              | 入网渠道类型        | string       | 16                               |
| IS_CAMP_USER              | 是否校园用户        | string       | 97                               |
| IS_CAMP_AREA_USER         | 是否校园区域用户    | string       | 78                               |
| IS_GROUP_USER             | 是否集团用户        | string       | 88                               |
| MEMB_TYP                  | 成员类型            | string       | 99                               |
| IS_GROUP_KEY_INDV         | 是否集团关键人      | string       | 30                               |
| IS_GSM_USER               | 是否全球通用户      | string       | 44                               |
| GSM_USER_LVL              | 全球通用户级别      | string       | 59                               |
| INNET_DURA_LVL_CODE       | 在网时长层次编码    | string       | 81                               |
| USER_AREA_BELO            | 用户区域归属        | string       | 22                               |
| ONNET_ALL_FLUX            | 上网全部流量(M)     | string       | 22                               |
| WDAY_ONNET_FLUX           | 工作日上网流量(M)   | string       | 91                               |
| NWDAY_ONNET_FLUX          | 非工作日上网流量(M) | string       | 10                               |
| ONNET_FLUX_3G             | 上网流量3G(M)       | string       | 55                               |
| ONNET_FLUX_4G             | 上网流量4G(M)       | string       | 68                               |
| TOT_FLUX_5G               | 总流量5G(M)         | string       | 54                               |
| FLUX_FEE                  | 流量费用            | string       | 53                               |
| FLUX_FEE_4G               | 流量费用4G          | string       | 63                               |
| FLUX_TOT_FEE_5G           | 流量总费用5G        | string       | 75                               |
| CHARGE_PACKAGE_UNIFY_CODE | 资费套餐统一编码    | string       | 75                               |
| CHARGE_PACKAGE_TYP        | 资费套餐类型        | string       | 75                               |
| PACK_MON                  | 套餐月费            | string       | 91                               |
| PACKAGE_5G_ID             | 5G套餐类型          | string       | 94                               |

# 2. 业务结果

业务需要基于上面产生的3个表，最终生成如下的结果。

| **图特征**                    | **特征含义**                                     |
| ----------------------------- | ------------------------------------------------ |
| MSISDN                        | 近2周新入网号码                                  |
| 1_HOP_NEI_COUNT               | 新号码的一度邻居数量                             |
| CALL_OTHER_USER_COUNT         | 新号码有过联系的其他自己号码的数量               |
| 1_HOP_CONNECT_NEI_COUNT       | 新号码的一度邻居中有互相联系的邻居数量           |
| USERS_COMMON_NEI_COUNT        | 新号码与其他自己号码的共同一度邻居数量           |
| USERS_1_HOP_NEI_CONNECT_COUNT | 新号码与其他自己号码的一度邻居的共同一度好友数量 |
| WADY_ONNET_FLUX_PREF_DIFF     | 新号码与其他自己号码的工作日上网流量偏好差异     |
| NWADY_ONNET_FLUX_PREF_DIFF    | 新号码与其他自己号码的非工作日上网流量偏好差异   |
| FLUX_FEE_PREF_DIFF            | 新号码与其他自己号码的流量费用偏好差异           |
| PACK_MON_PREF_DIFF            | 新号码与其他自己号码的套餐费用偏好差异           |

其中，MSISDN是非计算产生的，而CALL_OTHER_USER_COUNT/CALL_OTHER_USER_COUNT/1_HOP_CONNECT_NEI_COUNT/USERS_COMMON_NEI_COUNT/USERS_1_HOP_NEI_CONNECT_COUNT都是基于图结构上进行图分析得到的。剩余的偏好差异则主要是计算其他号码的平均值和新号码的当前值进行减法（基于tv表）。

# 3. 测试方案

## 3.1 数据生成

在现有业务上，数据是存在hdfs上面，然后导出一些字段生成新表，新表落成txt用于后续的加载分析。

对于HelmDB，则可以免去这些中间步骤，而是直接在原始完整数据表上进行分析。那因此，数据生成的目标就是生成原始的数据表。

具体的数据生成上，根据章节1.1中的原始表字段来生成数据。这个过程中需要注意以下几点。

- 生成用户和手机号码要和数据特征相匹配，即注意加密性和长短匹配。
- 在用户生成上，应该生成一些不同类型的用户来体现差异，比如有些用户有较多新手机号有些用户有较多旧手机号等。
- 在通话关系上，应该有一些差异性，考虑有些用户新旧手机号通话不同以及用户之间通话与否不同。

在数据规模上，需要根据HelmDB的特点，生成相对较多的数据，这样可以体现出缩短流程上产生的数据和比起来基于内存的图分析有着不同的数据规模支持能力。

### 3.1.1 用户画像分层设计

为了模拟真实的反欺诈场景，需要生成不同类型的用户画像：

**正常用户 (70%)**

- 长期用户：入网时长>12个月，通话稳定
- 新入网正常用户：入网时长≤2周，但通话模式正常

**可疑用户 (20%)**

- "实名不实人"嫌疑：同一身份证多个号码，通话模式相似
- 新入网高活跃用户：入网时长≤2周，一度邻居数量异常多
- 号码批量入网：同一时间段、同一渠道入网的多个号码

**欺诈用户 (10%)**

- 多个新号码共享联系人
- 新旧号码通话模式差异大
- 工作日/非工作日流量偏好异常

### 3.1.2 手机号和身份证生成规则

基于数据加密特性和真实数据特征：

**加密字段规则**

- MSISDN（手机号）：MD5加密后的32位字符串
- USER_ID：UUID或MD5生成的32位字符串
- IDTY_CODE（身份证）：MD5加密后的32位字符串

**身份证与号码的关联关系**

- 正常用户：1个身份证对应1-2个号码
- 可疑用户：1个身份证对应3-5个号码
- 欺诈用户：1个身份证对应5-10个号码

### 3.1.3 通话关系生成策略

**图结构特征**

- 正常用户：一度邻居数10-50，通话对象相对固定
- 可疑用户：一度邻居数50-200，新旧号码有部分共同邻居
- 欺诈用户：一度邻居数200+，新号码与旧号码有大量共同邻居（模拟批量换号）

**通话模式**

- 新旧号码共同邻居数：正常用户0-20%，欺诈用户50-80%
- 邻居间互相联系：正常用户10-30%，欺诈团伙用户60-90%

### 3.1.4 时间维度设计

**统计周期**

- 建议生成最近30天的数据
- 新入网定义：近2周（14天）内入网的用户

**入网时间分布**

- 70%历史用户（入网1个月以上）
- 20%近期用户（入网2-4周）
- 10%新用户（入网≤2周）

### 3.1.5 数据规模设计

基于HelmDB的特点，建议生成以下规模的数据：

**用户规模**

- 总用户数：10万-100万用户
- 通话记录：每用户每天平均5-20条通话记录
- 总通话记录数：千万级到亿级

**聚合表规模**

- user表：与用户数相同（10万-100万）
- call表：去重后的通话关系（百万-千万级）
- tv表：与用户数相同

### 3.1.6 字段数据生成规则

**枚举类型字段**

从固定值集合中随机选择（如性别、是否实名等）

**数值类型字段**

根据用户类型设置不同的分布范围：

- 通话时长：正态分布，均值300秒
- 流量使用：正态分布，均值5000M
- 各时段通话次数：符合真实场景分布（工作时间更多）

**关联字段**

确保逻辑一致性：

- RCN_DATE → RCN_DURA（入网日期推算入网时长）
- IDTY_CODE → SAME_IDCARD_MSISDN_CNT（同一身份证的号码数）
- STATIS_YMD：所有表保持时间一致性

### 3.1.7 数据一致性约束

生成过程中需要保证以下一致性约束：

- 同一USER_ID在不同表中的MSISDN必须一致
- call表中的MSISDN和OPP_MSISDN必须在user表中存在
- 时间字段保持一致（STATIS_YMD）
- 省份编码保持一致（PROV_ID）
- 身份证关联的号码数与实际生成的号码数匹配

## 3.2 数据分析

数据分析上应当完成和原始业务一样的事情，具体有两类。

1. 在图结构上进行分析，完成相应计算。
2. 偏好差异计算，这一块儿不太涉及到图分析，应该是偏关系的计算，即计算均值然后坐减法。

需要注意的是，这一块儿是否具备性能优势也有待讨论，因为相对于原始的NetworkX这种纯内存的分析，可能不会有显著的性能优势。