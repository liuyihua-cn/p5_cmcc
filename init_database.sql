-- ============================================
-- 移动反欺诈场景测试数据库初始化脚本
-- 适用于 OpenGauss 数据库
-- ============================================

-- 创建数据库（如果不存在）
-- 注意：在OpenGauss中，如果已连接到其他数据库，需要先断开再创建
DROP DATABASE IF EXISTS antifraud_test;
CREATE DATABASE antifraud_test;

-- 连接到新创建的数据库
\c antifraud_test;

-- ============================================
-- 1. 原始数据表
-- ============================================

-- 1.1 用户通话交际圈汇总日表
DROP TABLE IF EXISTS ty_m_unreal_person_call_data_filter;
CREATE TABLE ty_m_unreal_person_call_data_filter (
    STATIS_YMD VARCHAR(8),                    -- 统计日期（年月日）
    MSISDN VARCHAR(32),                        -- 移动号码（MD5加密）
    PROV_ID VARCHAR(10),                       -- 省份标识
    CALL_TYP_CODE VARCHAR(10),                 -- 呼叫类型编码
    OPP_MSISDN VARCHAR(32),                    -- 对方号码（MD5加密）
    OPP_BELO_CITY VARCHAR(50),                 -- 对端归属地市
    OPP_ISP_BRAND_CODE VARCHAR(10),            -- 对端运营商品牌编码
    CALL_DURA_SEC DECIMAL(18,0),               -- 通话时长(秒)
    H6_CALL_DURA_SEC DECIMAL(18,0),            -- 6点通话时长(秒)
    H7_CALL_DURA_SEC DECIMAL(18,0),            -- 7点通话时长(秒)
    H8_CALL_DURA_SEC DECIMAL(18,0),            -- 8点通话时长(秒)
    H9_CALL_DURA_SEC DECIMAL(18,0),            -- 9点通话时长(秒)
    H10_CALL_DURA_SEC DECIMAL(18,0),           -- 10点通话时长(秒)
    H11_CALL_DURA_SEC DECIMAL(18,0),           -- 11点通话时长(秒)
    H12_CALL_DURA_SEC DECIMAL(18,0),           -- 12点通话时长(秒)
    H13_CALL_DURA_SEC DECIMAL(18,0),           -- 13点通话时长(秒)
    H14_CALL_DURA_SEC DECIMAL(18,0),           -- 14点通话时长(秒)
    H15_CALL_DURA_SEC DECIMAL(18,0),           -- 15点通话时长(秒)
    H16_CALL_DURA_SEC DECIMAL(18,0),           -- 16点通话时长(秒)
    H17_CALL_DURA_SEC DECIMAL(18,0),           -- 17点通话时长(秒)
    H18_CALL_DURA_SEC DECIMAL(18,0),           -- 18点通话时长(秒)
    H19_CALL_DURA_SEC DECIMAL(18,0),           -- 19点通话时长(秒)
    H20_CALL_DURA_SEC DECIMAL(18,0),           -- 20点通话时长(秒)
    H21_CALL_DURA_SEC DECIMAL(18,0),           -- 21点通话时长(秒)
    H22_CALL_DURA_SEC DECIMAL(18,0),           -- 22点通话时长(秒)
    H23_H6_CALL_DURA_SEC DECIMAL(18,0),        -- 23点-6点通话时长(秒)
    BILL_DURA_MINU DECIMAL(18,4),              -- 计费时长（分钟）
    TOLL_BILL_DURA_SEC DECIMAL(18,0),          -- 长途计费时长（秒）
    FAV_FLW_CALL_INC_YUAN DECIMAL(18,4),       -- 优惠后通话费
    FAV_FLW_CFEE_YUAN DECIMAL(18,4),           -- 折扣后基本费
    FAV_FLW_TOLL_FEE_YUAN DECIMAL(18,4),       -- 折扣后长途费用
    FAV_FLW_OTHER_FEE_YUAN DECIMAL(18,4),      -- 优惠后其他费
    CALL_CNT DECIMAL(18,0),                    -- 通话次数
    H6_CALL_CNT DECIMAL(18,0),                 -- 6点通话次数
    H7_CALL_CNT DECIMAL(18,0),                 -- 7点通话次数
    H8_CALL_CNT DECIMAL(18,0),                 -- 8点通话次数
    H9_CALL_CNT DECIMAL(18,0),                 -- 9点通话次数
    H10_CALL_CNT DECIMAL(18,0),                -- 10点通话次数
    H11_CALL_CNT DECIMAL(18,0),                -- 11点通话次数
    H12_CALL_CNT DECIMAL(18,0),                -- 12点通话次数
    H13_CALL_CNT DECIMAL(18,0),                -- 13点通话次数
    H14_CALL_CNT DECIMAL(18,0),                -- 14点通话次数
    H15_CALL_CNT DECIMAL(18,0),                -- 15点通话次数
    H16_CALL_CNT DECIMAL(18,0),                -- 16点通话次数
    H17_CALL_CNT DECIMAL(18,0),                -- 17点通话次数
    H18_CALL_CNT DECIMAL(18,0),                -- 18点通话次数
    H19_CALL_CNT DECIMAL(18,0),                -- 19点通话次数
    H20_CALL_CNT DECIMAL(18,0),                -- 20点通话次数
    H21_CALL_CNT DECIMAL(18,0),                -- 21点通话次数
    H22_CALL_CNT DECIMAL(18,0),                -- 22点通话次数
    H23_H6_CALL_CNT DECIMAL(18,0),             -- 23点-6点通话次数
    CDR_SRC VARCHAR(50)                        -- 话单来源
);

-- 创建索引
CREATE INDEX idx_call_msisdn ON ty_m_unreal_person_call_data_filter(MSISDN);
CREATE INDEX idx_call_opp_msisdn ON ty_m_unreal_person_call_data_filter(OPP_MSISDN);
CREATE INDEX idx_call_date ON ty_m_unreal_person_call_data_filter(STATIS_YMD);

-- 1.2 用户基础信息日表及月表
DROP TABLE IF EXISTS ty_m_unreal_person_user_number_data_filter;
CREATE TABLE ty_m_unreal_person_user_number_data_filter (
    STATIS_YMD VARCHAR(8),                     -- 统计日期（年月日）
    PROV_ID VARCHAR(10),                       -- 省份标识
    CITY_CODE VARCHAR(10),                     -- 地市编码
    USER_ID VARCHAR(32),                       -- 用户标识
    CUST_ID VARCHAR(32),                       -- 客户标识
    NBR_PAR VARCHAR(10),                       -- 号段
    IS_PRETTY_NUM VARCHAR(2),                  -- 是否靓号
    PRETTY_NUM_TYP VARCHAR(10),                -- 靓号类型
    VIP_CUST_ID VARCHAR(10),                   -- 重要客户标识
    VIP_LVL VARCHAR(10),                       -- 会员级别
    INDV_CUST_LVL_CODE VARCHAR(10),            -- 个人客户级别编码
    AGE VARCHAR(3),                            -- 年龄
    AGE_LVL VARCHAR(10),                       -- 年龄分档
    SEX VARCHAR(2),                            -- 性别
    IDTY_TYP VARCHAR(10),                      -- 证件类型
    IDTY_CODE VARCHAR(32),                     -- 证件编号（MD5加密）
    IDCARD_BIRTH_AREA VARCHAR(20),             -- 身份证出生区域
    IDCARD_BIRTH_DATE VARCHAR(8),              -- 身份证出生日期
    SAME_IDCARD_MSISDN_CNT INT,                -- 同一身份证移动号码数
    NANL VARCHAR(20),                          -- 民族
    OCPN_CODE VARCHAR(10),                     -- 职业编码
    EDUCAT_DEGREE_CODE VARCHAR(10),            -- 教育程度编码
    USER_TYP VARCHAR(10),                      -- 用户类型
    BUSI_TYP VARCHAR(10),                      -- 业务类型
    PAY_TYP VARCHAR(10),                       -- 付费类型
    BRAND_ID VARCHAR(10),                      -- 品牌标识
    DATA_SIM_AND_M2M_USER_FLAG VARCHAR(2),     -- 数据SIM卡及M2M用户标志
    IS_M2M_ID_TD_SPECI VARCHAR(2),             -- 是否物联网标识(TD口径)
    MSISDN VARCHAR(32),                        -- 移动号码（MD5加密）
    IMSI VARCHAR(32),                          -- IMSI号
    IS_RNAME VARCHAR(2),                       -- 是否实名
    RCN_DATE VARCHAR(8),                       -- 入网日期
    RCN_CHNL_ID VARCHAR(10),                   -- 入网渠道标识
    RCN_CHNL_TYP VARCHAR(10),                  -- 入网渠道类型
    RCN_MODE VARCHAR(10),                      -- 入网方式
    IS_CAMP_USER VARCHAR(2),                   -- 是否校园用户
    IS_CAMP_AREA_USER VARCHAR(2),              -- 是否校园区域用户
    BELO_CAMP_ID VARCHAR(10),                  -- 所属校园标识
    IS_GROUP_USER VARCHAR(2),                  -- 是否集团用户
    MEMB_TYP VARCHAR(10),                      -- 成员类型
    IS_GROUP_KEY_INDV VARCHAR(2),              -- 是否集团关键人
    BECOME_GROUP_USER_MEMB_TM VARCHAR(14),     -- 成为集团用户成员时间
    GROUP_INDUS_TYP_CODE VARCHAR(10),          -- 集团行业分类编码
    GROUP_USER_UNIPAY_FLAG VARCHAR(2),         -- 集团用户统一付费标志
    BELO_GROUP_CUST_ID VARCHAR(32),            -- 归属集团客户标识
    IS_GSM_USER VARCHAR(2),                    -- 是否是全球通用户
    GSM_USER_LVL VARCHAR(10),                  -- 全球通用户级别
    GSM_USER_SRC VARCHAR(10),                  -- 全球通用户来源
    USER_STATUS VARCHAR(10),                   -- 用户状态
    STATUS_CHNG_DATE VARCHAR(8),               -- 状态变更日期
    IS_THIS_INNET VARCHAR(2),                  -- 是否本期在网
    INNET_DURA INT,                            -- 在网时长
    INNET_DURA_LVL_CODE VARCHAR(10),           -- 在网时长层次编码
    IS_CM_NADD VARCHAR(2),                     -- 是否本月新增
    IS_THIS_EXIT VARCHAR(2),                   -- 是否本期离网
    IS_FUSE_BRD VARCHAR(2),                    -- 是否融合宽带
    IS_THIS_STP VARCHAR(2),                    -- 是否本期停机
    LAST_ONE_STP_TM VARCHAR(14),               -- 最后一次停机时间
    STP_DURA INT,                              -- 停机时长
    STP_TYP VARCHAR(10),                       -- 停机类型
    CANCL_DATE VARCHAR(8),                     -- 销帐日期
    MSISDN_OWE_STP_FREQ INT,                   -- 手机号码欠费停机频次
    EXIT_TYP VARCHAR(10),                      -- 离网类型
    YEAR_NADD_ID VARCHAR(2),                   -- 年新增标识
    IS_ENJOY_GSM VARCHAR(2),                   -- 是否尊享全球通
    UNIQUE_FLAG VARCHAR(2),                    -- 唯一性标记
    USER_AREA_BELO VARCHAR(10),                -- 用户区域归属
    IS_GIVE_CARD VARCHAR(2),                   -- 是否赠送卡片
    PRETTY_NUM_TYP_NAME VARCHAR(50),           -- 靓号类型名称
    IS_CM_NADD_GIVE_CARD VARCHAR(2),           -- 是否本月新增赠送卡片
    IS_ASS_CARD VARCHAR(2),                    -- 是否副卡
    CUST_NAME VARCHAR(100),                    -- 客户姓名
    CNTY_CODE VARCHAR(10),                     -- 区县编码
    MAGE_STATUS VARCHAR(10),                   -- 婚姻状况
    NATION VARCHAR(50),                        -- 国籍
    BIND_BRD_CNT INT                           -- 绑定宽带笔数
);

-- 创建索引
CREATE INDEX idx_user_msisdn ON ty_m_unreal_person_user_number_data_filter(MSISDN);
CREATE INDEX idx_user_id ON ty_m_unreal_person_user_number_data_filter(USER_ID);
CREATE INDEX idx_user_idty_code ON ty_m_unreal_person_user_number_data_filter(IDTY_CODE);
CREATE INDEX idx_user_date ON ty_m_unreal_person_user_number_data_filter(STATIS_YMD);

-- 1.3 单一用户流量信息月表
DROP TABLE IF EXISTS tv_m_cust_single_flux_used;
CREATE TABLE tv_m_cust_single_flux_used (
    MSISDN VARCHAR(32),                        -- 移动号码（MD5加密）
    ONNET_FLUX_INCL_VOLTE DECIMAL(18,2),       -- 上网流量包含VOLTE
    ONNET_ALL_FLUX DECIMAL(18,2),              -- 上网全部流量
    WDAY_ONNET_FLUX_INCL_VOLTE DECIMAL(18,2),  -- 工作日上网流量包含VOLTE
    WDAY_ONNET_FLUX DECIMAL(18,2),             -- 工作日上网流量
    WDAY_H1_ONNET_FLUX DECIMAL(18,2),          -- 工作日1点上网流量
    ONNET_FLUX_4G DECIMAL(18,2),               -- 上网流量4G
    GPRS_ACES_DURA DECIMAL(18,2),              -- GPRS连接时间长
    NWDAY_ONNET_FLUX_INCL_VOLTE DECIMAL(18,2), -- 非工作日上网流量(含VOLTE)
    NWDAY_ONNET_FLUX DECIMAL(18,2),            -- 非工作日上网流量
    FLUX_VOLTE DECIMAL(18,2),                  -- 流量VOLTE
    UP_FLUX_VOLTE DECIMAL(18,2),               -- 上行流量VOLTE
    FLUX_M2M DECIMAL(18,2),                    -- 流量物联网
    OTHER_NTW_TYP_FLUX DECIMAL(18,2),          -- 其他网络类型流量
    CMWAP_FLUX DECIMAL(18,2),                  -- CMWAP流量
    CMNET_FLUX DECIMAL(18,2),                  -- CMNET流量
    LOC_ONNET_FLUX DECIMAL(18,2),              -- 本地上网流量
    ROAM_ONNET_FLUX DECIMAL(18,2),             -- 漫游上网流量
    FLUX_DURA_M2M DECIMAL(18,2),               -- 流量时长物联网
    LOC_ONNET_DURA DECIMAL(18,2),              -- 本地上网时长
    ROAM_ONNET_DURA DECIMAL(18,2),             -- 漫游上网时长
    ONNET_CNT DECIMAL(18,0),                   -- 上网次数
    LOC_ONNET_CNT DECIMAL(18,0),               -- 本地上网次数
    ROAM_ONNET_CNT DECIMAL(18,0),              -- 漫游上网次数
    ONNET_DAYS DECIMAL(18,0),                  -- 上网天数
    ROAM_FLUX_DAYS DECIMAL(18,0),              -- 漫游流量天数
    ONNET_FLUX_3G DECIMAL(18,2),               -- 上网流量3G
    TOT_FLUX_5G DECIMAL(18,2),                 -- 总流量5G
    FLUX_FE DECIMAL(18,2),                     -- 流量费用
    FLUX_FEE_4G DECIMAL(18,2),                 -- 流量费用4G
    FLUX_TOT_FEE_5G DECIMAL(18,2)              -- 流量总费用5G
);

-- 创建索引
CREATE INDEX idx_flux_msisdn ON tv_m_cust_single_flux_used(MSISDN);

-- 1.4 资费套餐订购日明细表
DROP TABLE IF EXISTS tw_d_is_pack_users_new_used;
CREATE TABLE tw_d_is_pack_users_new_used (
    USER_ID VARCHAR(32),                       -- 用户标识
    MSISDN VARCHAR(32),                        -- 用户号码（MD5加密）
    CITY_CODE VARCHAR(10),                     -- 地市
    CHARGE_PACKAGE_UNIFY_CODE VARCHAR(50),     -- 资费套餐统一编码
    PACKAGE_HADL_TM VARCHAR(14),               -- 套餐办理时间
    ORD_STATUS_TYP_CODE VARCHAR(10),           -- 资费订购状态
    CHARGE_PACKAGE_TYP VARCHAR(10),            -- 资费套餐类型
    PACK_FEE VARCHAR(20),                      -- 资费套餐月费
    HADL_CHNL_ID VARCHAR(10),                  -- 办理渠道全网统一编码
    IS_CUR_HADL VARCHAR(2),                    -- 是否当日办理
    IS_USE VARCHAR(2),                         -- 是否使用
    IS_ARRIVE VARCHAR(2),                      -- 是否到达
    IS_NOLIMIT VARCHAR(2),                     -- 是否不限量
    IS_RENT_CARD VARCHAR(2),                   -- 是否日租卡
    IS_FLY_ENJOY VARCHAR(2),                   -- 是否飞享
    IS_REACH_AMT_NO_SPEED VARCHAR(2),          -- 是否达量不限速
    IS_FAMPACK VARCHAR(2),                     -- 是否全家享套餐用户
    IS_FAMGIFT VARCHAR(2),                     -- 是否全家享礼包用户
    IS_5G_PACKAGE VARCHAR(2),                  -- 是否5G套餐
    PACKAGE_5G_ID VARCHAR(10),                 -- 5g套餐类型
    STATIS_YMD VARCHAR(8),                     -- 统计日（年月日）
    PROV_ID VARCHAR(10),                       -- 省份标识
    PACK_MON VARCHAR(20)                       -- 套餐月费
);

-- 创建索引
CREATE INDEX idx_pack_msisdn ON tw_d_is_pack_users_new_used(MSISDN);
CREATE INDEX idx_pack_user_id ON tw_d_is_pack_users_new_used(USER_ID);
CREATE INDEX idx_pack_date ON tw_d_is_pack_users_new_used(STATIS_YMD);

-- ============================================
-- 2. 聚合数据表
-- ============================================

-- 2.1 user表（用户信息，用于构建点）
DROP TABLE IF EXISTS user_graph;
CREATE TABLE user_graph (
    MSISDN VARCHAR(32),                        -- 手机号码（MD5加密）
    USER_ID VARCHAR(32),                       -- 用户唯一标识
    NEW_RCN_ID VARCHAR(2),                     -- 是否新入网用户（1表示是）
    RCN_DURA VARCHAR(10),                      -- 入网时长（月）
    IDTY_NBR VARCHAR(32),                      -- 身份证号（MD5加密）
    STATIS_YMD VARCHAR(8),                     -- 统计日期
    PROV_ID VARCHAR(10),                       -- 省份编码
    PRIMARY KEY (MSISDN, STATIS_YMD)
);

-- 创建索引
CREATE INDEX idx_user_graph_user_id ON user_graph(USER_ID);
CREATE INDEX idx_user_graph_idty ON user_graph(IDTY_NBR);

-- 2.2 call表（通话关系，用于构建边）
DROP TABLE IF EXISTS call_graph;
CREATE TABLE call_graph (
    MSISDN VARCHAR(32),                        -- 主叫手机号（MD5加密）
    OPP_MSISDN VARCHAR(32),                    -- 被叫手机号（MD5加密）
    STATIS_YMD VARCHAR(8),                     -- 通话日期
    PROV_ID VARCHAR(10)                        -- 省份编码
);

-- 创建索引
CREATE INDEX idx_call_graph_msisdn ON call_graph(MSISDN);
CREATE INDEX idx_call_graph_opp ON call_graph(OPP_MSISDN);
CREATE INDEX idx_call_graph_date ON call_graph(STATIS_YMD);

-- 2.3 tv表（宽表，用来分析用户特征变化）
DROP TABLE IF EXISTS tv_analysis;
CREATE TABLE tv_analysis (
    USER_ID VARCHAR(32),                       -- 用户唯一标识
    MSISDN VARCHAR(32),                        -- 手机号码（MD5加密）
    IS_PRETTY_NUM VARCHAR(2),                  -- 是否靓号
    PRETTY_NUM_TYP VARCHAR(10),                -- 靓号类型
    VIP_CUST_ID VARCHAR(10),                   -- 重要客户标识
    VIP_LVL VARCHAR(10),                       -- 会员级别
    AGE_LVL VARCHAR(10),                       -- 年龄分档
    SEX VARCHAR(2),                            -- 性别
    OCPN_CODE VARCHAR(10),                     -- 职业编码
    EDUCAT_DEGREE_CODE VARCHAR(10),            -- 教育程度编码
    BRAND_ID VARCHAR(10),                      -- 品牌标识
    RCN_CHNL_TYP VARCHAR(10),                  -- 入网渠道类型
    IS_CAMP_USER VARCHAR(2),                   -- 是否校园用户
    IS_CAMP_AREA_USER VARCHAR(2),              -- 是否校园区域用户
    IS_GROUP_USER VARCHAR(2),                  -- 是否集团用户
    MEMB_TYP VARCHAR(10),                      -- 成员类型
    IS_GROUP_KEY_INDV VARCHAR(2),              -- 是否集团关键人
    IS_GSM_USER VARCHAR(2),                    -- 是否全球通用户
    GSM_USER_LVL VARCHAR(10),                  -- 全球通用户级别
    INNET_DURA_LVL_CODE VARCHAR(10),           -- 在网时长层次编码
    USER_AREA_BELO VARCHAR(10),                -- 用户区域归属
    ONNET_ALL_FLUX VARCHAR(20),                -- 上网全部流量(M)
    WDAY_ONNET_FLUX VARCHAR(20),               -- 工作日上网流量(M)
    NWDAY_ONNET_FLUX VARCHAR(20),              -- 非工作日上网流量(M)
    ONNET_FLUX_3G VARCHAR(20),                 -- 上网流量3G(M)
    ONNET_FLUX_4G VARCHAR(20),                 -- 上网流量4G(M)
    TOT_FLUX_5G VARCHAR(20),                   -- 总流量5G(M)
    FLUX_FEE VARCHAR(20),                      -- 流量费用
    FLUX_FEE_4G VARCHAR(20),                   -- 流量费用4G
    FLUX_TOT_FEE_5G VARCHAR(20),               -- 流量总费用5G
    CHARGE_PACKAGE_UNIFY_CODE VARCHAR(50),     -- 资费套餐统一编码
    CHARGE_PACKAGE_TYP VARCHAR(10),            -- 资费套餐类型
    PACK_MON VARCHAR(20),                      -- 套餐月费
    PACKAGE_5G_ID VARCHAR(10),                 -- 5G套餐类型
    PRIMARY KEY (USER_ID, MSISDN)
);

-- 创建索引
CREATE INDEX idx_tv_msisdn ON tv_analysis(MSISDN);

-- ============================================
-- 数据库初始化完成
-- ============================================
