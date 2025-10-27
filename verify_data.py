#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据验证脚本
检查生成的数据是否符合预期
"""

import csv
import os
from collections import defaultdict

def verify_call_data(filename):
    """验证通话记录数据"""
    print("=" * 60)
    print("验证通话记录数据")
    print("=" * 60)

    if not os.path.exists(filename):
        print(f"❌ 文件不存在: {filename}")
        return

    # 统计数据
    total_calls = 0
    self_calls = 0  # 自己打给自己的次数
    user_call_counts = defaultdict(int)  # 每个用户的通话次数
    msisdn_pairs = set()  # 去重的通话对

    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_calls += 1
            msisdn = row['MSISDN']
            opp_msisdn = row['OPP_MSISDN']

            # 检查是否自己打给自己
            if msisdn == opp_msisdn:
                self_calls += 1
                print(f"⚠️  发现自己打给自己: {msisdn}")

            # 统计每个用户的通话次数
            user_call_counts[msisdn] += 1

            # 记录通话对
            msisdn_pairs.add((msisdn, opp_msisdn))

    # 输出统计结果
    print(f"\n总通话记录数: {total_calls:,}")
    print(f"唯一通话对数: {len(msisdn_pairs):,}")
    print(f"唯一用户数: {len(user_call_counts):,}")

    # 检查自己打给自己的情况
    if self_calls > 0:
        print(f"\n❌ 发现 {self_calls} 条自己打给自己的记录！")
    else:
        print(f"\n✅ 没有发现自己打给自己的记录")

    # 分析每用户通话次数分布
    call_counts = list(user_call_counts.values())
    if call_counts:
        min_calls = min(call_counts)
        max_calls = max(call_counts)
        avg_calls = sum(call_counts) / len(call_counts)

        print(f"\n每用户通话次数统计:")
        print(f"  最小值: {min_calls}")
        print(f"  最大值: {max_calls}")
        print(f"  平均值: {avg_calls:.2f}")
        print(f"  范围比: {max_calls / min_calls:.2f}x" if min_calls > 0 else "  范围比: N/A")

        # 检查差异度
        if max_calls / min_calls >= 2.5:
            print(f"  ✅ 通话数量差异度良好（{max_calls / min_calls:.2f}x >= 2.5x）")
        else:
            print(f"  ⚠️  通话数量差异度较小（{max_calls / min_calls:.2f}x < 2.5x）")

        # 显示分布直方图（简化版）
        print(f"\n通话次数分布:")
        bins = {}
        for count in call_counts:
            bin_key = (count // 5) * 5  # 按5分组
            bins[bin_key] = bins.get(bin_key, 0) + 1

        for bin_key in sorted(bins.keys()):
            bar = '█' * (bins[bin_key] // max(len(call_counts) // 50, 1))
            print(f"  [{bin_key:2d}-{bin_key+4:2d}]: {bins[bin_key]:4d} {bar}")

    print("=" * 60)


def verify_user_data(filename):
    """验证用户数据"""
    print("\n验证用户数据")
    print("=" * 60)

    if not os.path.exists(filename):
        print(f"❌ 文件不存在: {filename}")
        return

    total_users = 0
    unique_msisdn = set()
    unique_user_id = set()
    unique_idty = set()

    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_users += 1
            unique_msisdn.add(row['MSISDN'])
            unique_user_id.add(row['USER_ID'])
            unique_idty.add(row['IDTY_CODE'])

    print(f"总用户数: {total_users:,}")
    print(f"唯一手机号: {len(unique_msisdn):,}")
    print(f"唯一用户ID: {len(unique_user_id):,}")
    print(f"唯一身份证: {len(unique_idty):,}")

    # 检查唯一性
    if total_users == len(unique_msisdn) == len(unique_user_id):
        print("✅ 手机号和用户ID无重复")
    else:
        print("❌ 发现重复的手机号或用户ID")

    print("=" * 60)


def main():
    """主函数"""
    # 验证通话记录
    call_file = "./output_data/ty_m_unreal_person_call_data_filter.csv"
    verify_call_data(call_file)

    # 验证用户数据
    user_file = "./output_data/ty_m_unreal_person_user_number_data_filter.csv"
    verify_user_data(user_file)

    print("\n验证完成！")


if __name__ == "__main__":
    main()
