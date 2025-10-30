-- ============================================
-- 移动反欺诈场景测试数据导入脚本
-- 适用于 OpenGauss/GaussDB 数据库
-- ============================================
-- 使用说明：
-- 1. 执行前请确保：
--    - 数据库 antifraud_test 已创建（通过 init_database.sql）
--    - CSV 文件已生成在 output_data 目录中
-- 2. 在项目根目录下执行：gsql -d antifraud_test -p 5432 -U username -f import_data.sql
-- ============================================

-- 连接到数据库
\c antifraud_test;

-- ============================================
-- 导入原始数据
-- ============================================

-- 1. 导入用户基础信息数据
\echo '导入用户基础信息数据...'
COPY ty_m_unreal_person_user_number_data_filter
FROM 'output_data/ty_m_unreal_person_user_number_data_filter.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

-- 2. 导入通话记录数据
\echo '导入通话记录数据...'
COPY ty_m_unreal_person_call_data_filter
FROM 'output_data/ty_m_unreal_person_call_data_filter.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

-- 3. 导入流量数据
\echo '导入流量数据...'
COPY tv_m_cust_single_flux_used
FROM 'output_data/tv_m_cust_single_flux_used.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

-- 4. 导入资费套餐数据
\echo '导入资费套餐数据...'
COPY tw_d_is_pack_users_new_used
FROM 'output_data/tw_d_is_pack_users_new_used.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

\echo '数据导入完成！'
