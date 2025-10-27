#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据过滤脚本
将output_data中的原始CSV文件转换为filter_data中的目标TXT文件
根据README.md中定义的schema映射规则生成user.txt、call.txt、tv.txt
"""

import csv
import os
from datetime import datetime
from typing import Dict

# ============================================
# 配置参数
# ============================================

class FilterConfig:
    """过滤配置"""
    INPUT_DIR = "./output_data"      # 输入目录
    OUTPUT_DIR = "./filter_data"     # 输出目录

    # 输入文件名
    INPUT_USER_FILE = "ty_m_unreal_person_user_number_data_filter.csv"
    INPUT_CALL_FILE = "ty_m_unreal_person_call_data_filter.csv"
    INPUT_FLUX_FILE = "tv_m_cust_single_flux_used.csv"
    INPUT_PACKAGE_FILE = "tw_d_is_pack_users_new_used.csv"

    # 输出文件名
    OUTPUT_USER_FILE = "user.txt"
    OUTPUT_CALL_FILE = "call.txt"
    OUTPUT_TV_FILE = "tv.txt"

    # 输出格式配置（默认与cmcc/data格式一致）
    OUTPUT_DELIMITER = '€€'         # 分隔符（默认：€€，可改为'\t'或','等）
    OUTPUT_WITH_HEADER = False      # 是否输出表头（默认：False，可改为True）


# ============================================
# 工具函数
# ============================================

def calculate_months_diff(start_date: str, end_date: str) -> int:
    """
    计算两个日期之间的月份差

    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        月份差
    """
    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        return (end.year - start.year) * 12 + end.month - start.month
    except:
        return 0


def safe_get(row: Dict, key: str, default: str = '') -> str:
    """
    安全获取字典值

    Args:
        row: 数据行字典
        key: 键名
        default: 默认值

    Returns:
        值或默认值
    """
    value = row.get(key, default)
    return str(value).strip() if value else default


# ============================================
# 数据过滤类
# ============================================

class DataFilter:
    """数据过滤器"""

    def __init__(self, config: FilterConfig):
        self.config = config

        # 用于存储中间数据
        self.flux_data: Dict[str, Dict] = {}  # MSISDN -> 流量数据
        self.package_data: Dict[str, Dict] = {}  # MSISDN -> 套餐数据

    def run(self):
        """运行数据过滤流程"""
        print("=" * 60)
        print("数据过滤工具")
        print("=" * 60)
        print()

        # 创建输出目录
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)

        # 检查输入文件
        if not self._check_input_files():
            return

        # 加载辅助数据（流量和套餐）
        print("步骤1: 加载辅助数据...")
        self._load_flux_data()
        self._load_package_data()

        # 生成user.txt
        print("步骤2: 生成user.txt...")
        self._generate_user_file()

        # 生成call.txt
        print("步骤3: 生成call.txt...")
        self._generate_call_file()

        # 生成tv.txt
        print("步骤4: 生成tv.txt...")
        self._generate_tv_file()

        print()
        print("=" * 60)
        print("数据过滤完成！")
        print(f"输出目录: {self.config.OUTPUT_DIR}")
        print("=" * 60)

    def _check_input_files(self) -> bool:
        """检查输入文件是否存在"""
        required_files = [
            self.config.INPUT_USER_FILE,
            self.config.INPUT_CALL_FILE,
            self.config.INPUT_FLUX_FILE,
            self.config.INPUT_PACKAGE_FILE
        ]

        missing_files = []
        for filename in required_files:
            filepath = os.path.join(self.config.INPUT_DIR, filename)
            if not os.path.exists(filepath):
                missing_files.append(filename)

        if missing_files:
            print(f"错误：以下文件不存在:")
            for filename in missing_files:
                print(f"  - {filename}")
            print(f"\n请先运行 generate_test_data.py 生成数据")
            return False

        return True

    def _load_flux_data(self):
        """加载流量数据到内存"""
        filepath = os.path.join(self.config.INPUT_DIR, self.config.INPUT_FLUX_FILE)

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                msisdn = safe_get(row, 'MSISDN')
                if msisdn:
                    self.flux_data[msisdn] = row

        print(f"  ✓ 加载流量数据: {len(self.flux_data):,} 条")

    def _load_package_data(self):
        """加载套餐数据到内存（每个用户取第一个套餐）"""
        filepath = os.path.join(self.config.INPUT_DIR, self.config.INPUT_PACKAGE_FILE)

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                msisdn = safe_get(row, 'MSISDN')
                # 只保存每个用户的第一个套餐
                if msisdn and msisdn not in self.package_data:
                    self.package_data[msisdn] = row

        print(f"  ✓ 加载套餐数据: {len(self.package_data):,} 条")

    def _generate_user_file(self):
        """
        生成user.txt文件

        字段顺序：
        MSISDN, USER_ID, NEW_RCN_ID, RCN_DURA, IDTY_NBR, STATIS_YMD, PROV_ID
        """
        input_path = os.path.join(self.config.INPUT_DIR, self.config.INPUT_USER_FILE)
        output_path = os.path.join(self.config.OUTPUT_DIR, self.config.OUTPUT_USER_FILE)

        row_count = 0

        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)

            # 写入表头（如果配置需要）
            if self.config.OUTPUT_WITH_HEADER:
                headers = ['MSISDN', 'USER_ID', 'NEW_RCN_ID', 'RCN_DURA', 'IDTY_NBR', 'STATIS_YMD', 'PROV_ID']
                outfile.write(self.config.OUTPUT_DELIMITER.join(headers) + '\n')

            # 处理数据
            for row in reader:
                msisdn = safe_get(row, 'MSISDN')
                user_id = safe_get(row, 'USER_ID')
                rcn_date = safe_get(row, 'RCN_DATE')
                statis_ymd = safe_get(row, 'STATIS_YMD')
                idty_code = safe_get(row, 'IDTY_CODE')
                prov_id = safe_get(row, 'PROV_ID')

                # 计算入网时长（月）
                rcn_dura = calculate_months_diff(rcn_date, statis_ymd)

                # 判断是否新入网用户（≤2周，约0.5个月）
                new_rcn_id = '1' if rcn_dura <= 0.5 else '0'

                # 写入数据行
                output_row = [
                    msisdn,
                    user_id,
                    new_rcn_id,
                    str(rcn_dura),
                    idty_code,  # IDTY_CODE映射为IDTY_NBR
                    statis_ymd,
                    prov_id
                ]

                outfile.write(self.config.OUTPUT_DELIMITER.join(output_row) + '\n')
                row_count += 1

        print(f"  ✓ 生成 {self.config.OUTPUT_USER_FILE}: {row_count:,} 条记录")

    def _generate_call_file(self):
        """
        生成call.txt文件

        字段顺序：
        MSISDN, OPP_MSISDN, STATIS_YMD, PROV_ID
        """
        input_path = os.path.join(self.config.INPUT_DIR, self.config.INPUT_CALL_FILE)
        output_path = os.path.join(self.config.OUTPUT_DIR, self.config.OUTPUT_CALL_FILE)

        row_count = 0

        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)

            # 写入表头（如果配置需要）
            if self.config.OUTPUT_WITH_HEADER:
                headers = ['MSISDN', 'OPP_MSISDN', 'STATIS_YMD', 'PROV_ID']
                outfile.write(self.config.OUTPUT_DELIMITER.join(headers) + '\n')

            # 处理数据（去重）
            seen_calls = set()

            for row in reader:
                msisdn = safe_get(row, 'MSISDN')
                opp_msisdn = safe_get(row, 'OPP_MSISDN')
                statis_ymd = safe_get(row, 'STATIS_YMD')
                prov_id = safe_get(row, 'PROV_ID')

                # 去重：同一天的同一对通话关系只保留一次
                call_key = (msisdn, opp_msisdn, statis_ymd)
                if call_key in seen_calls:
                    continue

                seen_calls.add(call_key)

                # 写入数据行
                output_row = [msisdn, opp_msisdn, statis_ymd, prov_id]
                outfile.write(self.config.OUTPUT_DELIMITER.join(output_row) + '\n')
                row_count += 1

        print(f"  ✓ 生成 {self.config.OUTPUT_CALL_FILE}: {row_count:,} 条记录（已去重）")

    def _generate_tv_file(self):
        """
        生成tv.txt文件

        字段顺序（34个字段）：
        USER_ID, MSISDN,
        IS_PRETTY_NUM, PRETTY_NUM_TYP, VIP_CUST_ID, VIP_LVL, AGE_LVL, SEX,
        OCPN_CODE, EDUCAT_DEGREE_CODE, BRAND_ID, RCN_CHNL_TYP,
        IS_CAMP_USER, IS_CAMP_AREA_USER, IS_GROUP_USER, MEMB_TYP, IS_GROUP_KEY_INDV,
        IS_GSM_USER, GSM_USER_LVL, INNET_DURA_LVL_CODE, USER_AREA_BELO,
        ONNET_ALL_FLUX, WDAY_ONNET_FLUX, NWDAY_ONNET_FLUX,
        ONNET_FLUX_3G, ONNET_FLUX_4G, TOT_FLUX_5G,
        FLUX_FEE, FLUX_FEE_4G, FLUX_TOT_FEE_5G,
        CHARGE_PACKAGE_UNIFY_CODE, CHARGE_PACKAGE_TYP, PACK_MON, PACKAGE_5G_ID
        """
        input_path = os.path.join(self.config.INPUT_DIR, self.config.INPUT_USER_FILE)
        output_path = os.path.join(self.config.OUTPUT_DIR, self.config.OUTPUT_TV_FILE)

        row_count = 0

        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:

            reader = csv.DictReader(infile)

            # 写入表头（如果配置需要）
            if self.config.OUTPUT_WITH_HEADER:
                headers = [
                    'USER_ID', 'MSISDN',
                    'IS_PRETTY_NUM', 'PRETTY_NUM_TYP', 'VIP_CUST_ID', 'VIP_LVL',
                    'AGE_LVL', 'SEX', 'OCPN_CODE', 'EDUCAT_DEGREE_CODE',
                    'BRAND_ID', 'RCN_CHNL_TYP', 'IS_CAMP_USER', 'IS_CAMP_AREA_USER',
                    'IS_GROUP_USER', 'MEMB_TYP', 'IS_GROUP_KEY_INDV',
                    'IS_GSM_USER', 'GSM_USER_LVL', 'INNET_DURA_LVL_CODE', 'USER_AREA_BELO',
                    'ONNET_ALL_FLUX', 'WDAY_ONNET_FLUX', 'NWDAY_ONNET_FLUX',
                    'ONNET_FLUX_3G', 'ONNET_FLUX_4G', 'TOT_FLUX_5G',
                    'FLUX_FEE', 'FLUX_FEE_4G', 'FLUX_TOT_FEE_5G',
                    'CHARGE_PACKAGE_UNIFY_CODE', 'CHARGE_PACKAGE_TYP', 'PACK_MON', 'PACKAGE_5G_ID'
                ]
                outfile.write(self.config.OUTPUT_DELIMITER.join(headers) + '\n')

            # 处理数据
            for row in reader:
                msisdn = safe_get(row, 'MSISDN')

                # 从用户基础信息表获取字段
                user_id = safe_get(row, 'USER_ID')
                is_pretty_num = safe_get(row, 'IS_PRETTY_NUM')
                pretty_num_typ = safe_get(row, 'PRETTY_NUM_TYP')
                vip_cust_id = safe_get(row, 'VIP_CUST_ID')
                vip_lvl = safe_get(row, 'VIP_LVL')
                age_lvl = safe_get(row, 'AGE_LVL')
                sex = safe_get(row, 'SEX')
                ocpn_code = safe_get(row, 'OCPN_CODE')
                educat_degree_code = safe_get(row, 'EDUCAT_DEGREE_CODE')
                brand_id = safe_get(row, 'BRAND_ID')
                rcn_chnl_typ = safe_get(row, 'RCN_CHNL_TYP')
                is_camp_user = safe_get(row, 'IS_CAMP_USER')
                is_camp_area_user = safe_get(row, 'IS_CAMP_AREA_USER')
                is_group_user = safe_get(row, 'IS_GROUP_USER')
                memb_typ = safe_get(row, 'MEMB_TYP')
                is_group_key_indv = safe_get(row, 'IS_GROUP_KEY_INDV')
                is_gsm_user = safe_get(row, 'IS_GSM_USER')
                gsm_user_lvl = safe_get(row, 'GSM_USER_LVL')
                innet_dura_lvl_code = safe_get(row, 'INNET_DURA_LVL_CODE')
                user_area_belo = safe_get(row, 'USER_AREA_BELO')

                # 从流量信息表获取字段
                flux_row = self.flux_data.get(msisdn, {})
                onnet_all_flux = safe_get(flux_row, 'ONNET_ALL_FLUX', '0')
                wday_onnet_flux = safe_get(flux_row, 'WDAY_ONNET_FLUX', '0')
                nwday_onnet_flux = safe_get(flux_row, 'NWDAY_ONNET_FLUX', '0')
                onnet_flux_3g = safe_get(flux_row, 'ONNET_FLUX_3G', '0')
                onnet_flux_4g = safe_get(flux_row, 'ONNET_FLUX_4G', '0')
                tot_flux_5g = safe_get(flux_row, 'TOT_FLUX_5G', '0')
                flux_fee = safe_get(flux_row, 'FLUX_FE', '0')  # 注意：字段名是FLUX_FE
                flux_fee_4g = safe_get(flux_row, 'FLUX_FEE_4G', '0')
                flux_tot_fee_5g = safe_get(flux_row, 'FLUX_TOT_FEE_5G', '0')

                # 从套餐信息表获取字段
                package_row = self.package_data.get(msisdn, {})
                charge_package_unify_code = safe_get(package_row, 'CHARGE_PACKAGE_UNIFY_CODE', '')
                charge_package_typ = safe_get(package_row, 'CHARGE_PACKAGE_TYP', '')
                pack_mon = safe_get(package_row, 'PACK_MON', '')
                package_5g_id = safe_get(package_row, 'PACKAGE_5G_ID', '')

                # 写入数据行
                output_row = [
                    user_id, msisdn,
                    is_pretty_num, pretty_num_typ, vip_cust_id, vip_lvl,
                    age_lvl, sex, ocpn_code, educat_degree_code,
                    brand_id, rcn_chnl_typ, is_camp_user, is_camp_area_user,
                    is_group_user, memb_typ, is_group_key_indv,
                    is_gsm_user, gsm_user_lvl, innet_dura_lvl_code, user_area_belo,
                    onnet_all_flux, wday_onnet_flux, nwday_onnet_flux,
                    onnet_flux_3g, onnet_flux_4g, tot_flux_5g,
                    flux_fee, flux_fee_4g, flux_tot_fee_5g,
                    charge_package_unify_code, charge_package_typ, pack_mon, package_5g_id
                ]

                outfile.write(self.config.OUTPUT_DELIMITER.join(output_row) + '\n')
                row_count += 1

        print(f"  ✓ 生成 {self.config.OUTPUT_TV_FILE}: {row_count:,} 条记录")


# ============================================
# 主函数
# ============================================

def main():
    """主函数"""
    filter_tool = DataFilter(FilterConfig)
    filter_tool.run()


if __name__ == "__main__":
    main()
