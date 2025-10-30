#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
移动反欺诈测试数据生成脚本
根据用户画像分层生成模拟数据
"""

import hashlib
import random
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Set
import os

# ============================================
# 配置参数 - 可以修改这些参数来调整数据规模和特征
# ============================================

class Config:
    """数据生成配置"""

    # 数据规模配置
    TOTAL_USERS = 100              # 总用户数（建议：10000-1000000）
    CALLS_PER_USER_PER_MONTH = 8     # 每个用户每月平均通话次数（建议：5-10）

    # 用户画像比例
    NORMAL_USER_RATIO = 0.70         # 正常用户比例 70%
    SUSPICIOUS_USER_RATIO = 0.20     # 可疑用户比例 20%
    FRAUD_USER_RATIO = 0.10          # 欺诈用户比例 10%

    # 新入网用户比例
    NEW_USER_RATIO = 0.10            # 新入网用户（≤2周）比例 10%
    RECENT_USER_RATIO = 0.20         # 近期用户（2-4周）比例 20%
    OLD_USER_RATIO = 0.70            # 历史用户（>1个月）比例 70%

    # 通话关系配置
    NEIGHBOR_COUNT = {
        'normal': (10, 50),          # 正常用户一度邻居数范围
        'suspicious': (50, 200),     # 可疑用户一度邻居数范围
        'fraud': (200, 500)          # 欺诈用户一度邻居数范围
    }

    # 共同邻居比例
    COMMON_NEIGHBOR_RATIO = {
        'normal': (0.0, 0.2),        # 正常用户新旧号码共同邻居比例
        'suspicious': (0.2, 0.5),    # 可疑用户
        'fraud': (0.5, 0.8)          # 欺诈用户
    }

    # 身份证对应号码数
    ID_CARD_PHONE_COUNT = {
        'normal': (1, 2),            # 正常用户
        'suspicious': (3, 5),        # 可疑用户
        'fraud': (5, 10)             # 欺诈用户
    }

    # 省份和地市配置
    PROV_ID = "10000"                # 省份标识
    CITY_CODES = ["0101", "0102", "0103", "0104", "0105"]

    # 数据月份配置（生成当前月的数据）
    # 如果想生成上个月的数据，设置为1；生成当前月设置为0
    MONTH_OFFSET = 0                 # 0=当前月，1=上个月，2=上上个月...

    # 输出目录
    OUTPUT_DIR = "./output_data"


# ============================================
# 工具函数
# ============================================

def generate_md5(text: str) -> str:
    """生成MD5哈希值"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def generate_phone_number(existing_phones: Set[str] = None) -> str:
    """
    生成唯一的手机号码MD5

    Args:
        existing_phones: 已存在的手机号集合，用于去重

    Returns:
        MD5加密的手机号
    """
    max_attempts = 1000
    for _ in range(max_attempts):
        phone = f"1{random.randint(3, 9)}{random.randint(100000000, 999999999)}"
        phone_md5 = generate_md5(phone)

        if existing_phones is None or phone_md5 not in existing_phones:
            if existing_phones is not None:
                existing_phones.add(phone_md5)
            return phone_md5

    # 如果随机生成失败，使用递增方式保证唯一性
    import uuid
    return generate_md5(f"PHONE_{uuid.uuid4()}")

def generate_user_id(existing_ids: Set[str] = None) -> str:
    """
    生成唯一的用户ID

    Args:
        existing_ids: 已存在的用户ID集合，用于去重

    Returns:
        MD5加密的用户ID
    """
    import uuid
    max_attempts = 1000
    for _ in range(max_attempts):
        user_id = generate_md5(str(uuid.uuid4()))

        if existing_ids is None or user_id not in existing_ids:
            if existing_ids is not None:
                existing_ids.add(user_id)
            return user_id

    # UUID碰撞概率极低，但为了保险
    return generate_md5(f"USER_{uuid.uuid4()}_{random.randint(0, 999999)}")

def generate_id_card(existing_ids: Set[str] = None) -> str:
    """
    生成唯一的身份证号MD5（使用真实日期格式）

    Args:
        existing_ids: 已存在的身份证号集合，用于去重

    Returns:
        MD5加密的身份证号
    """
    max_attempts = 1000
    for _ in range(max_attempts):
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
            # 简化处理，不考虑闰年
            max_day = 28
        birth_day = random.randint(1, max_day)
        birth_date = f"{birth_year:04d}{birth_month:02d}{birth_day:02d}"

        # 顺序码（3位）+ 校验码（1位）
        sequence = random.randint(100, 999)
        check_digit = random.randint(0, 9)

        id_card = f"{region_code}{birth_date}{sequence}{check_digit}"
        id_card_md5 = generate_md5(id_card)

        if existing_ids is None or id_card_md5 not in existing_ids:
            if existing_ids is not None:
                existing_ids.add(id_card_md5)
            return id_card_md5

    # 如果随机生成失败，使用UUID保证唯一性
    import uuid
    return generate_md5(f"ID_{uuid.uuid4()}")

def calculate_months_diff(start_date: str, end_date: str) -> int:
    """计算两个日期之间的月份差"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    return (end.year - start.year) * 12 + end.month - start.month

def get_date_n_days_ago(base_date: str, n: int) -> str:
    """获取n天前的日期"""
    date = datetime.strptime(base_date, "%Y%m%d")
    return (date - timedelta(days=n)).strftime("%Y%m%d")

def get_month_date_range(month_offset: int = 0) -> tuple:
    """
    获取指定月份的日期范围（月初到月底）

    Args:
        month_offset: 月份偏移（0=当前月，1=上个月，2=上上个月...）

    Returns:
        (start_date, end_date, days_in_month) 格式为YYYYMMDD
    """
    from calendar import monthrange

    today = datetime.now()

    # 计算目标年月
    target_year = today.year
    target_month = today.month - month_offset

    # 处理跨年情况
    while target_month < 1:
        target_month += 12
        target_year -= 1

    # 获取该月的天数
    days_in_month = monthrange(target_year, target_month)[1]

    # 生成开始和结束日期
    start_date = f"{target_year:04d}{target_month:02d}01"
    end_date = f"{target_year:04d}{target_month:02d}{days_in_month:02d}"

    return start_date, end_date, days_in_month


# ============================================
# 用户生成类
# ============================================

class User:
    """用户实体"""
    def __init__(self, user_type: str, user_category: str, existing_phones: Set[str] = None, existing_user_ids: Set[str] = None):
        self.user_id = generate_user_id(existing_user_ids)
        self.msisdn = generate_phone_number(existing_phones)
        self.user_type = user_type  # normal, suspicious, fraud
        self.user_category = user_category  # new, recent, old
        self.id_card = None
        self.rcn_date = None
        self.rcn_dura = None
        self.neighbors = []  # 一度邻居列表

    def set_id_card(self, id_card: str):
        """设置身份证"""
        self.id_card = id_card

    def set_rcn_info(self, rcn_date: str, end_date: str):
        """设置入网信息"""
        self.rcn_date = rcn_date
        self.rcn_dura = calculate_months_diff(rcn_date, end_date)

    def is_new_user(self) -> bool:
        """是否是新入网用户（≤2周，约0.5个月）"""
        return self.rcn_dura <= 0.5


class DataGenerator:
    """数据生成器"""

    def __init__(self, config: Config):
        self.config = config
        self.users: List[User] = []
        self.id_cards_map: Dict[str, List[User]] = {}  # 身份证 -> 用户列表

        # 去重集合
        self.existing_phones: Set[str] = set()  # 已生成的手机号
        self.existing_user_ids: Set[str] = set()  # 已生成的用户ID
        self.existing_id_cards: Set[str] = set()  # 已生成的身份证号

    def generate_users(self):
        """生成用户"""
        print(f"开始生成 {self.config.TOTAL_USERS} 个用户...")

        # 计算各类用户数量
        normal_count = int(self.config.TOTAL_USERS * self.config.NORMAL_USER_RATIO)
        suspicious_count = int(self.config.TOTAL_USERS * self.config.SUSPICIOUS_USER_RATIO)
        fraud_count = self.config.TOTAL_USERS - normal_count - suspicious_count

        # 生成正常用户
        self._generate_user_group('normal', normal_count)

        # 生成可疑用户
        self._generate_user_group('suspicious', suspicious_count)

        # 生成欺诈用户
        self._generate_user_group('fraud', fraud_count)

        print(f"用户生成完成！正常用户: {normal_count}, 可疑用户: {suspicious_count}, 欺诈用户: {fraud_count}")

    def _generate_user_group(self, user_type: str, count: int):
        """生成一组特定类型的用户"""
        # 获取当前月（或指定月份）的结束日期作为基准
        _start_date, end_date, _days_in_month = get_month_date_range(self.config.MONTH_OFFSET)

        # 计算各入网时间类别的数量
        new_count = int(count * self.config.NEW_USER_RATIO)
        recent_count = int(count * self.config.RECENT_USER_RATIO)
        old_count = count - new_count - recent_count

        categories = [
            ('new', new_count),
            ('recent', recent_count),
            ('old', old_count)
        ]

        for category, cat_count in categories:
            for _ in range(cat_count):
                # 传入去重集合
                user = User(user_type, category, self.existing_phones, self.existing_user_ids)

                # 设置入网日期（基于当前月的结束日期）
                if category == 'new':
                    days_ago = random.randint(1, 14)  # 1-14天前
                elif category == 'recent':
                    days_ago = random.randint(15, 30)  # 15-30天前
                else:
                    days_ago = random.randint(31, 365)  # 31-365天前

                rcn_date = get_date_n_days_ago(end_date, days_ago)
                user.set_rcn_info(rcn_date, end_date)

                # 分配身份证（根据用户类型决定是共享还是独立）
                self._assign_id_card(user)

                self.users.append(user)

    def _assign_id_card(self, user: User):
        """为用户分配身份证"""
        _min_phones, max_phones = self.config.ID_CARD_PHONE_COUNT[user.user_type]

        # 随机决定是否创建新身份证
        create_new = True

        # 对于可疑和欺诈用户，尝试找到现有身份证共享
        if user.user_type in ['suspicious', 'fraud']:
            # 查找可以共享的身份证（还没达到号码数上限的）
            for id_card, users_with_id in self.id_cards_map.items():
                if len(users_with_id) < max_phones:
                    # 有一定概率共享这个身份证
                    if random.random() < 0.7:  # 70%概率共享
                        user.set_id_card(id_card)
                        users_with_id.append(user)
                        create_new = False
                        break

        # 创建新身份证（使用去重集合）
        if create_new:
            new_id_card = generate_id_card(self.existing_id_cards)
            user.set_id_card(new_id_card)
            self.id_cards_map[new_id_card] = [user]

    def generate_call_relationships(self):
        """生成通话关系"""
        print("开始生成通话关系...")

        for user in self.users:
            # 根据用户类型确定邻居数量
            min_neighbors, max_neighbors = self.config.NEIGHBOR_COUNT[user.user_type]
            neighbor_count = random.randint(min_neighbors, max_neighbors)

            # 为该用户选择邻居（排除自己）
            # 邻居可能是其他用户，也可能是不在用户列表中的号码
            other_users = [u for u in self.users if u.user_id != user.user_id]
            user_neighbors = random.sample(other_users, min(neighbor_count // 2, len(other_users)))

            # 添加一些外部号码（使用去重集合）
            external_count = neighbor_count - len(user_neighbors)
            for _ in range(external_count):
                external_phone = generate_phone_number(self.existing_phones)
                user_neighbors.append(external_phone)

            user.neighbors = user_neighbors

            # 对于欺诈用户，如果同一身份证有多个号码，创建共同邻居
            if user.user_type == 'fraud' and user.id_card in self.id_cards_map:
                same_id_users = self.id_cards_map[user.id_card]
                if len(same_id_users) > 1:
                    # 计算共同邻居比例
                    min_ratio, max_ratio = self.config.COMMON_NEIGHBOR_RATIO[user.user_type]
                    common_ratio = random.uniform(min_ratio, max_ratio)

                    # 创建共同邻居
                    common_count = int(len(user.neighbors) * common_ratio)
                    for other_user in same_id_users:
                        if other_user.user_id != user.user_id and other_user.neighbors:
                            # 共享一些邻居
                            shared = random.sample(user.neighbors, min(common_count, len(user.neighbors)))
                            other_user.neighbors.extend(shared)

        print("通话关系生成完成！")

    def export_to_csv(self):
        """导出数据到CSV文件"""
        print("开始导出数据到CSV文件...")

        # 创建输出目录
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)

        # 导出用户基础信息表
        self._export_user_base_info()

        # 导出通话记录表
        self._export_call_records()

        # 导出流量信息表
        self._export_flux_info()

        # 导出套餐信息表
        self._export_package_info()

        print(f"数据导出完成！输出目录: {self.config.OUTPUT_DIR}")

    def _export_user_base_info(self):
        """导出用户基础信息日表"""
        # 获取当前月（或指定月份）的结束日期
        _start_date, end_date, _days_in_month = get_month_date_range(self.config.MONTH_OFFSET)

        filename = os.path.join(self.config.OUTPUT_DIR, "ty_m_unreal_person_user_number_data_filter.csv")

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            headers = [
                'STATIS_YMD', 'PROV_ID', 'CITY_CODE', 'USER_ID', 'CUST_ID', 'NBR_PAR',
                'IS_PRETTY_NUM', 'PRETTY_NUM_TYP', 'VIP_CUST_ID', 'VIP_LVL',
                'INDV_CUST_LVL_CODE', 'AGE', 'AGE_LVL', 'SEX', 'IDTY_TYP', 'IDTY_CODE',
                'IDCARD_BIRTH_AREA', 'IDCARD_BIRTH_DATE', 'SAME_IDCARD_MSISDN_CNT',
                'NANL', 'OCPN_CODE', 'EDUCAT_DEGREE_CODE', 'USER_TYP', 'BUSI_TYP',
                'PAY_TYP', 'BRAND_ID', 'DATA_SIM_AND_M2M_USER_FLAG', 'IS_M2M_ID_TD_SPECI',
                'MSISDN', 'IMSI', 'IS_RNAME', 'RCN_DATE', 'NEW_RCN_ID', 'RCN_DURA',
                'RCN_CHNL_ID', 'RCN_CHNL_TYP',
                'RCN_MODE', 'IS_CAMP_USER', 'IS_CAMP_AREA_USER', 'BELO_CAMP_ID',
                'IS_GROUP_USER', 'MEMB_TYP', 'IS_GROUP_KEY_INDV', 'BECOME_GROUP_USER_MEMB_TM',
                'GROUP_INDUS_TYP_CODE', 'GROUP_USER_UNIPAY_FLAG', 'BELO_GROUP_CUST_ID',
                'IS_GSM_USER', 'GSM_USER_LVL', 'GSM_USER_SRC', 'USER_STATUS',
                'STATUS_CHNG_DATE', 'IS_THIS_INNET', 'INNET_DURA', 'INNET_DURA_LVL_CODE',
                'IS_CM_NADD', 'IS_THIS_EXIT', 'IS_FUSE_BRD', 'IS_THIS_STP',
                'LAST_ONE_STP_TM', 'STP_DURA', 'STP_TYP', 'CANCL_DATE',
                'MSISDN_OWE_STP_FREQ', 'EXIT_TYP', 'YEAR_NADD_ID', 'IS_ENJOY_GSM',
                'UNIQUE_FLAG', 'USER_AREA_BELO', 'IS_GIVE_CARD', 'PRETTY_NUM_TYP_NAME',
                'IS_CM_NADD_GIVE_CARD', 'IS_ASS_CARD', 'CUST_NAME', 'CNTY_CODE',
                'MAGE_STATUS', 'NATION', 'BIND_BRD_CNT'
            ]
            writer.writerow(headers)

            # 写入数据
            for user in self.users:
                same_id_count = len(self.id_cards_map.get(user.id_card, []))
                age = random.randint(18, 65)

                row = [
                    end_date,              # STATIS_YMD（当前月的结束日期）
                    self.config.PROV_ID,   # PROV_ID
                    random.choice(self.config.CITY_CODES),  # CITY_CODE
                    user.user_id,          # USER_ID
                    generate_user_id(self.existing_user_ids),    # CUST_ID
                    user.msisdn[:3],       # NBR_PAR
                    random.choice(['0', '1']),  # IS_PRETTY_NUM
                    random.randint(1, 10), # PRETTY_NUM_TYP
                    random.randint(1, 100), # VIP_CUST_ID
                    random.randint(1, 5),  # VIP_LVL
                    random.randint(1, 5),  # INDV_CUST_LVL_CODE
                    age,                   # AGE
                    str(age // 10),        # AGE_LVL
                    random.choice(['1', '2']),  # SEX (1=男, 2=女)
                    '01',                  # IDTY_TYP (身份证)
                    user.id_card,          # IDTY_CODE
                    str(random.randint(110000, 650000)),  # IDCARD_BIRTH_AREA
                    user.rcn_date,         # IDCARD_BIRTH_DATE (简化处理)
                    same_id_count,         # SAME_IDCARD_MSISDN_CNT
                    '01',                  # NANL (汉族)
                    random.randint(1, 20), # OCPN_CODE
                    random.randint(1, 8),  # EDUCAT_DEGREE_CODE
                    '1',                   # USER_TYP
                    '1',                   # BUSI_TYP
                    random.choice(['1', '2']),  # PAY_TYP
                    random.randint(1, 5),  # BRAND_ID
                    '0',                   # DATA_SIM_AND_M2M_USER_FLAG
                    '0',                   # IS_M2M_ID_TD_SPECI
                    user.msisdn,           # MSISDN
                    generate_md5(f"IMSI{random.randint(100000000000000, 999999999999999)}"),  # IMSI
                    '1',                   # IS_RNAME (实名)
                    user.rcn_date,         # RCN_DATE
                    '1' if user.is_new_user() else '0',  # NEW_RCN_ID
                    user.rcn_dura,         # RCN_DURA
                    random.randint(1, 100), # RCN_CHNL_ID
                    random.randint(1, 10), # RCN_CHNL_TYP
                    '1',                   # RCN_MODE
                    random.choice(['0', '1']),  # IS_CAMP_USER
                    random.choice(['0', '1']),  # IS_CAMP_AREA_USER
                    random.randint(1, 50) if random.random() < 0.1 else '',  # BELO_CAMP_ID
                    random.choice(['0', '1']),  # IS_GROUP_USER
                    random.randint(1, 3),  # MEMB_TYP
                    random.choice(['0', '1']),  # IS_GROUP_KEY_INDV
                    '',                    # BECOME_GROUP_USER_MEMB_TM
                    random.randint(1, 20), # GROUP_INDUS_TYP_CODE
                    random.choice(['0', '1']),  # GROUP_USER_UNIPAY_FLAG
                    '',                    # BELO_GROUP_CUST_ID
                    random.choice(['0', '1']),  # IS_GSM_USER
                    random.randint(1, 5),  # GSM_USER_LVL
                    random.randint(1, 3),  # GSM_USER_SRC
                    '00',                  # USER_STATUS (在网)
                    end_date,              # STATUS_CHNG_DATE
                    '1',                   # IS_THIS_INNET
                    user.rcn_dura,         # INNET_DURA
                    str(min(user.rcn_dura // 6 + 1, 10)),  # INNET_DURA_LVL_CODE
                    '1' if user.is_new_user() else '0',  # IS_CM_NADD
                    '0',                   # IS_THIS_EXIT
                    random.choice(['0', '1']),  # IS_FUSE_BRD
                    '0',                   # IS_THIS_STP
                    '',                    # LAST_ONE_STP_TM
                    0,                     # STP_DURA
                    '',                    # STP_TYP
                    '',                    # CANCL_DATE
                    0,                     # MSISDN_OWE_STP_FREQ
                    '',                    # EXIT_TYP
                    '1' if user.is_new_user() else '0',  # YEAR_NADD_ID
                    random.choice(['0', '1']),  # IS_ENJOY_GSM
                    '1',                   # UNIQUE_FLAG
                    random.randint(1, 5),  # USER_AREA_BELO
                    random.choice(['0', '1']),  # IS_GIVE_CARD
                    '',                    # PRETTY_NUM_TYP_NAME
                    '0',                   # IS_CM_NADD_GIVE_CARD
                    random.choice(['0', '1']),  # IS_ASS_CARD
                    f"用户{random.randint(1000, 9999)}",  # CUST_NAME
                    random.choice(self.config.CITY_CODES) + str(random.randint(10, 99)),  # CNTY_CODE
                    random.choice(['1', '2', '3']),  # MAGE_STATUS
                    'CN',                  # NATION
                    random.randint(0, 3)   # BIND_BRD_CNT
                ]
                writer.writerow(row)

        print(f"  ✓ 用户基础信息表已导出: {filename}")

    def _export_call_records(self):
        """导出通话记录表"""
        filename = os.path.join(self.config.OUTPUT_DIR, "ty_m_unreal_person_call_data_filter.csv")

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            headers = [
                'STATIS_YMD', 'MSISDN', 'PROV_ID', 'CALL_TYP_CODE', 'OPP_MSISDN',
                'OPP_BELO_CITY', 'OPP_ISP_BRAND_CODE', 'CALL_DURA_SEC',
                'H6_CALL_DURA_SEC', 'H7_CALL_DURA_SEC', 'H8_CALL_DURA_SEC', 'H9_CALL_DURA_SEC',
                'H10_CALL_DURA_SEC', 'H11_CALL_DURA_SEC', 'H12_CALL_DURA_SEC', 'H13_CALL_DURA_SEC',
                'H14_CALL_DURA_SEC', 'H15_CALL_DURA_SEC', 'H16_CALL_DURA_SEC', 'H17_CALL_DURA_SEC',
                'H18_CALL_DURA_SEC', 'H19_CALL_DURA_SEC', 'H20_CALL_DURA_SEC', 'H21_CALL_DURA_SEC',
                'H22_CALL_DURA_SEC', 'H23_H6_CALL_DURA_SEC', 'BILL_DURA_MINU', 'TOLL_BILL_DURA_SEC',
                'FAV_FLW_CALL_INC_YUAN', 'FAV_FLW_CFEE_YUAN', 'FAV_FLW_TOLL_FEE_YUAN',
                'FAV_FLW_OTHER_FEE_YUAN', 'CALL_CNT',
                'H6_CALL_CNT', 'H7_CALL_CNT', 'H8_CALL_CNT', 'H9_CALL_CNT',
                'H10_CALL_CNT', 'H11_CALL_CNT', 'H12_CALL_CNT', 'H13_CALL_CNT',
                'H14_CALL_CNT', 'H15_CALL_CNT', 'H16_CALL_CNT', 'H17_CALL_CNT',
                'H18_CALL_CNT', 'H19_CALL_CNT', 'H20_CALL_CNT', 'H21_CALL_CNT',
                'H22_CALL_CNT', 'H23_H6_CALL_CNT', 'CDR_SRC'
            ]
            writer.writerow(headers)

            # 获取当前月（或指定月份）的日期范围
            start_date, end_date, days_in_month = get_month_date_range(self.config.MONTH_OFFSET)

            # 生成日期列表
            month_dates = []
            base_date = datetime.strptime(start_date, "%Y%m%d")
            for i in range(days_in_month):
                month_dates.append((base_date + timedelta(days=i)).strftime("%Y%m%d"))

            # 写入数据 - 为每个用户生成每月的通话记录
            for user in self.users:
                # 每个用户每月的通话次数（在 0.5x 到 2x 范围内变化）
                min_calls = max(1, int(self.config.CALLS_PER_USER_PER_MONTH * 0.5))
                max_calls = int(self.config.CALLS_PER_USER_PER_MONTH * 2)
                monthly_calls = random.randint(min_calls, max_calls)

                # 从邻居中选择通话对象（排除自己）
                if user.neighbors:
                    # 过滤掉自己（User对象形式 或 手机号字符串形式）
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

                    if not valid_neighbors:
                        continue  # 如果没有有效邻居，跳过这个用户

                    call_targets = random.choices(
                        valid_neighbors,
                        k=min(monthly_calls, len(valid_neighbors))
                    )

                    for target in call_targets:
                        # 为每条通话记录随机选择一个日期（在整个月内）
                        call_date = random.choice(month_dates)

                        # 获取被叫号码
                        if isinstance(target, User):
                            opp_msisdn = target.msisdn
                        else:
                            opp_msisdn = target

                        # 生成通话时长分布（按小时）
                        hour_dura = [0] * 24
                        hour_cnt = [0] * 24

                        # 随机选择通话时段（工作时间更多）
                        call_hours = random.choices(
                            range(24),
                            weights=[1,1,1,1,1,2,3,4,5,6,7,7,6,5,5,6,7,8,8,7,6,4,3,2],
                            k=random.randint(1, 5)
                        )

                        total_dura = 0
                        for hour in call_hours:
                            dura = random.randint(30, 600)  # 30秒到10分钟
                            hour_dura[hour] = dura
                            hour_cnt[hour] = 1
                            total_dura += dura

                        row = [
                            call_date,             # STATIS_YMD
                            user.msisdn,           # MSISDN
                            self.config.PROV_ID,   # PROV_ID
                            random.choice(['1', '2', '3']),  # CALL_TYP_CODE
                            opp_msisdn,            # OPP_MSISDN
                            random.choice(self.config.CITY_CODES),  # OPP_BELO_CITY
                            random.choice(['1', '2', '3']),  # OPP_ISP_BRAND_CODE
                            total_dura,            # CALL_DURA_SEC
                        ]

                        # 添加各小时通话时长
                        row.extend([hour_dura[6], hour_dura[7], hour_dura[8], hour_dura[9],
                                   hour_dura[10], hour_dura[11], hour_dura[12], hour_dura[13],
                                   hour_dura[14], hour_dura[15], hour_dura[16], hour_dura[17],
                                   hour_dura[18], hour_dura[19], hour_dura[20], hour_dura[21],
                                   hour_dura[22], sum(hour_dura[23:6])])

                        # 添加计费信息
                        row.extend([
                            round(total_dura / 60, 2),  # BILL_DURA_MINU
                            random.randint(0, total_dura),  # TOLL_BILL_DURA_SEC
                            round(random.uniform(0.1, 2.0), 2),  # FAV_FLW_CALL_INC_YUAN
                            round(random.uniform(0.05, 1.0), 2),  # FAV_FLW_CFEE_YUAN
                            round(random.uniform(0, 0.5), 2),    # FAV_FLW_TOLL_FEE_YUAN
                            round(random.uniform(0, 0.3), 2),    # FAV_FLW_OTHER_FEE_YUAN
                            len(call_hours),       # CALL_CNT
                        ])

                        # 添加各小时通话次数
                        row.extend([hour_cnt[6], hour_cnt[7], hour_cnt[8], hour_cnt[9],
                                   hour_cnt[10], hour_cnt[11], hour_cnt[12], hour_cnt[13],
                                   hour_cnt[14], hour_cnt[15], hour_cnt[16], hour_cnt[17],
                                   hour_cnt[18], hour_cnt[19], hour_cnt[20], hour_cnt[21],
                                   hour_cnt[22], sum(hour_cnt[23:6])])

                        row.append('CDR_01')  # CDR_SRC

                        writer.writerow(row)

        print(f"  ✓ 通话记录表已导出: {filename}")

    def _export_flux_info(self):
        """导出流量信息表"""
        filename = os.path.join(self.config.OUTPUT_DIR, "tv_m_cust_single_flux_used.csv")

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            headers = [
                'MSISDN', 'ONNET_FLUX_INCL_VOLTE', 'ONNET_ALL_FLUX', 'WDAY_ONNET_FLUX_INCL_VOLTE',
                'WDAY_ONNET_FLUX', 'WDAY_H1_ONNET_FLUX', 'ONNET_FLUX_4G', 'GPRS_ACES_DURA',
                'NWDAY_ONNET_FLUX_INCL_VOLTE', 'NWDAY_ONNET_FLUX', 'FLUX_VOLTE', 'UP_FLUX_VOLTE',
                'FLUX_M2M', 'OTHER_NTW_TYP_FLUX', 'CMWAP_FLUX', 'CMNET_FLUX', 'LOC_ONNET_FLUX',
                'ROAM_ONNET_FLUX', 'FLUX_DURA_M2M', 'LOC_ONNET_DURA', 'ROAM_ONNET_DURA',
                'ONNET_CNT', 'LOC_ONNET_CNT', 'ROAM_ONNET_CNT', 'ONNET_DAYS', 'ROAM_FLUX_DAYS',
                'ONNET_FLUX_3G', 'TOT_FLUX_5G', 'FLUX_FE', 'FLUX_FEE_4G', 'FLUX_TOT_FEE_5G'
            ]
            writer.writerow(headers)

            # 为每个用户生成流量数据
            for user in self.users:
                # 基础流量（MB）- 根据用户类型有差异
                base_flux = random.normalvariate(5000, 2000)  # 均值5000MB，标准差2000MB
                base_flux = max(100, base_flux)  # 最小100MB

                wday_flux = base_flux * random.uniform(0.6, 0.8)  # 工作日流量60-80%
                nwday_flux = base_flux - wday_flux  # 非工作日流量

                flux_4g = base_flux * random.uniform(0.7, 0.9)  # 4G流量占70-90%
                flux_3g = base_flux * random.uniform(0.05, 0.15)  # 3G流量占5-15%
                flux_5g = base_flux * random.uniform(0.05, 0.25)  # 5G流量占5-25%

                row = [
                    user.msisdn,           # MSISDN
                    round(base_flux * 1.05, 2),  # ONNET_FLUX_INCL_VOLTE
                    round(base_flux, 2),   # ONNET_ALL_FLUX
                    round(wday_flux * 1.05, 2),  # WDAY_ONNET_FLUX_INCL_VOLTE
                    round(wday_flux, 2),   # WDAY_ONNET_FLUX
                    round(wday_flux * random.uniform(0.01, 0.05), 2),  # WDAY_H1_ONNET_FLUX
                    round(flux_4g, 2),     # ONNET_FLUX_4G
                    round(random.uniform(10000, 100000), 2),  # GPRS_ACES_DURA
                    round(nwday_flux * 1.05, 2),  # NWDAY_ONNET_FLUX_INCL_VOLTE
                    round(nwday_flux, 2),  # NWDAY_ONNET_FLUX
                    round(base_flux * 0.05, 2),  # FLUX_VOLTE
                    round(base_flux * 0.02, 2),  # UP_FLUX_VOLTE
                    0,                     # FLUX_M2M
                    round(base_flux * random.uniform(0, 0.1), 2),  # OTHER_NTW_TYP_FLUX
                    round(base_flux * random.uniform(0.1, 0.3), 2),  # CMWAP_FLUX
                    round(base_flux * random.uniform(0.7, 0.9), 2),  # CMNET_FLUX
                    round(base_flux * random.uniform(0.8, 0.95), 2),  # LOC_ONNET_FLUX
                    round(base_flux * random.uniform(0, 0.2), 2),    # ROAM_ONNET_FLUX
                    0,                     # FLUX_DURA_M2M
                    round(random.uniform(50000, 200000), 2),  # LOC_ONNET_DURA
                    round(random.uniform(0, 50000), 2),       # ROAM_ONNET_DURA
                    random.randint(50, 500),   # ONNET_CNT
                    random.randint(40, 400),   # LOC_ONNET_CNT
                    random.randint(0, 100),    # ROAM_ONNET_CNT
                    random.randint(20, 30),    # ONNET_DAYS
                    random.randint(0, 10),     # ROAM_FLUX_DAYS
                    round(flux_3g, 2),     # ONNET_FLUX_3G
                    round(flux_5g, 2),     # TOT_FLUX_5G
                    round(base_flux * random.uniform(0.001, 0.01), 2),  # FLUX_FE
                    round(flux_4g * random.uniform(0.001, 0.01), 2),    # FLUX_FEE_4G
                    round(flux_5g * random.uniform(0.001, 0.01), 2)     # FLUX_TOT_FEE_5G
                ]
                writer.writerow(row)

        print(f"  ✓ 流量信息表已导出: {filename}")

    def _export_package_info(self):
        """导出套餐信息表"""
        # 获取当前月（或指定月份）的结束日期
        _start_date, end_date, _days_in_month = get_month_date_range(self.config.MONTH_OFFSET)

        filename = os.path.join(self.config.OUTPUT_DIR, "tw_d_is_pack_users_new_used.csv")

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            headers = [
                'USER_ID', 'MSISDN', 'CITY_CODE', 'CHARGE_PACKAGE_UNIFY_CODE', 'PACKAGE_HADL_TM',
                'ORD_STATUS_TYP_CODE', 'CHARGE_PACKAGE_TYP', 'PACK_FEE', 'HADL_CHNL_ID',
                'IS_CUR_HADL', 'IS_USE', 'IS_ARRIVE', 'IS_NOLIMIT', 'IS_RENT_CARD',
                'IS_FLY_ENJOY', 'IS_REACH_AMT_NO_SPEED', 'IS_FAMPACK', 'IS_FAMGIFT',
                'IS_5G_PACKAGE', 'PACKAGE_5G_ID', 'STATIS_YMD', 'PROV_ID', 'PACK_MON'
            ]
            writer.writerow(headers)

            # 为每个用户生成套餐信息
            for user in self.users:
                # 每个用户可能有1-3个套餐
                package_count = random.randint(1, 3)

                for _ in range(package_count):
                    package_fee = random.choice([28, 38, 58, 88, 128, 158, 198, 298])
                    is_5g = random.choice(['0', '1']) if package_fee >= 128 else '0'

                    row = [
                        user.user_id,          # USER_ID
                        user.msisdn,           # MSISDN
                        random.choice(self.config.CITY_CODES),  # CITY_CODE
                        f"PKG{random.randint(100000, 999999)}",  # CHARGE_PACKAGE_UNIFY_CODE
                        user.rcn_date + "000000",  # PACKAGE_HADL_TM
                        '1',                   # ORD_STATUS_TYP_CODE (有效)
                        random.randint(1, 10), # CHARGE_PACKAGE_TYP
                        package_fee,           # PACK_FEE
                        random.randint(1, 100), # HADL_CHNL_ID
                        '0',                   # IS_CUR_HADL
                        '1',                   # IS_USE
                        '1',                   # IS_ARRIVE
                        random.choice(['0', '1']),  # IS_NOLIMIT
                        random.choice(['0', '1']),  # IS_RENT_CARD
                        random.choice(['0', '1']),  # IS_FLY_ENJOY
                        random.choice(['0', '1']),  # IS_REACH_AMT_NO_SPEED
                        random.choice(['0', '1']),  # IS_FAMPACK
                        random.choice(['0', '1']),  # IS_FAMGIFT
                        is_5g,                 # IS_5G_PACKAGE
                        random.randint(1, 5) if is_5g == '1' else '',  # PACKAGE_5G_ID
                        end_date,              # STATIS_YMD
                        self.config.PROV_ID,   # PROV_ID
                        package_fee            # PACK_MON
                    ]
                    writer.writerow(row)

        print(f"  ✓ 套餐信息表已导出: {filename}")


# ============================================
# 主函数
# ============================================

def main():
    """主函数"""
    print("=" * 60)
    print("移动反欺诈测试数据生成工具")
    print("=" * 60)
    print()

    # 获取生成月份信息
    start_date, end_date, days_in_month = get_month_date_range(Config.MONTH_OFFSET)

    # 显示配置信息
    print("当前配置:")
    print(f"  总用户数: {Config.TOTAL_USERS:,}")
    print(f"  数据月份: {start_date[:6]} ({start_date} 至 {end_date}，共{days_in_month}天)")
    print(f"  每用户每月通话次数: {Config.CALLS_PER_USER_PER_MONTH}")
    print(f"  正常用户比例: {Config.NORMAL_USER_RATIO * 100}%")
    print(f"  可疑用户比例: {Config.SUSPICIOUS_USER_RATIO * 100}%")
    print(f"  欺诈用户比例: {Config.FRAUD_USER_RATIO * 100}%")
    print(f"  输出目录: {Config.OUTPUT_DIR}")
    print()

    # 创建数据生成器
    generator = DataGenerator(Config)

    # 生成用户
    generator.generate_users()

    # 生成通话关系
    generator.generate_call_relationships()

    # 导出到CSV
    generator.export_to_csv()

    print()
    print("=" * 60)
    print("数据生成完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
