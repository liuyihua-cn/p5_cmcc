#!/bin/bash
# 实名不实人检测系统 - 智能运行脚本
# 自动检查和配置环境

set -e  # 遇到错误立即退出

# ============================================
# 配置区域
# ============================================
PYTHON_VERSION="3.13"
REQUIRED_PACKAGES="pyspark==4.0.1 networkx PyYAML"
JAVA_HOME_PATH="/opt/homebrew/opt/openjdk@17"

# ============================================
# 环境检查和配置函数
# ============================================

# 检查 Python 版本
check_python_version() {
    if ! command -v python3 &> /dev/null; then
        echo "❌ 错误: 未找到 python3"
        echo "请先安装 Python 3.13+"
        exit 1
    fi

    CURRENT_VERSION=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
    echo "✓ 找到 Python ${CURRENT_VERSION}"
}

# 检查 Java
check_java() {
    if [ ! -d "${JAVA_HOME_PATH}" ]; then
        echo "❌ 错误: 未找到 Java 17"
        echo "请运行: brew install openjdk@17"
        exit 1
    fi

    export PATH="${JAVA_HOME_PATH}/bin:$PATH"
    export JAVA_HOME="${JAVA_HOME_PATH}"
    echo "✓ Java 17 配置完成"
}

# 检查或创建虚拟环境
check_or_create_venv() {
    if [ ! -d "venv" ]; then
        echo ""
        echo "📦 未找到虚拟环境，正在创建..."
        python3 -m venv venv
        echo "✅ 虚拟环境创建成功"
    else
        echo "✓ 虚拟环境已存在"
    fi
}

# 激活虚拟环境
activate_venv() {
    if [ -z "$VIRTUAL_ENV" ]; then
        echo "🔄 激活虚拟环境..."
        source venv/bin/activate
        echo "✅ 虚拟环境已激活"
    else
        echo "✓ 虚拟环境已激活"
    fi
}

# 检查并安装依赖
check_or_install_packages() {
    echo ""
    echo "🔍 检查 Python 依赖包..."

    NEED_INSTALL=0

    # 检查 pyspark
    if ! python -c "import pyspark" 2>/dev/null; then
        echo "  ⚠️  需要安装 pyspark"
        NEED_INSTALL=1
    else
        PYSPARK_VERSION=$(python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null)
        echo "  ✓ pyspark ${PYSPARK_VERSION} 已安装"
    fi

    # 检查 networkx
    if ! python -c "import networkx" 2>/dev/null; then
        echo "  ⚠️  需要安装 networkx"
        NEED_INSTALL=1
    else
        echo "  ✓ networkx 已安装"
    fi

    # 检查 yaml
    if ! python -c "import yaml" 2>/dev/null; then
        echo "  ⚠️  需要安装 PyYAML"
        NEED_INSTALL=1
    else
        echo "  ✓ PyYAML 已安装"
    fi

    # 如果需要安装
    if [ $NEED_INSTALL -eq 1 ]; then
        echo ""
        echo "📥 正在安装依赖包..."
        pip install --quiet --upgrade pip
        pip install --quiet ${REQUIRED_PACKAGES}
        echo "✅ 依赖包安装完成"
    else
        echo "✅ 所有依赖包已就绪"
    fi
}

# 创建必要目录
create_directories() {
    mkdir -p logs
    mkdir -p save_models
    mkdir -p results
    mkdir -p results/inter
}

# ============================================
# 主程序
# ============================================

echo "=========================================="
echo "实名不实人检测系统 - 智能启动"
echo "=========================================="
echo ""

# 1. 检查 Python
check_python_version

# 2. 检查 Java
check_java

# 3. 检查/创建虚拟环境
check_or_create_venv

# 4. 激活虚拟环境
activate_venv

# 5. 检查/安装依赖
check_or_install_packages

# 6. 创建必要目录
create_directories

echo ""
echo "=========================================="
echo "环境准备完成，开始运行检测"
echo "=========================================="
echo ""

# ============================================
# 解析运行参数
# ============================================

# 默认参数（这些是唯一的配置来源）
PROVINCE="jiangsu"
MONTHID="202308"        # 改为与 generate_test_data.py 生成的月份一致
MODE="local"
MODEL_TYPE="nx"
ONLY_GRAPH="0"
ONLY_TV="0"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--province)
            PROVINCE="$2"
            shift 2
            ;;
        -m|--monthid)
            MONTHID="$2"
            shift 2
            ;;
        --only-graph)
            ONLY_GRAPH="1"
            shift
            ;;
        --only-tv)
            ONLY_TV="1"
            shift
            ;;
        -h|--help)
            echo "用法: ./run_detection.sh [选项]"
            echo ""
            echo "选项:"
            echo "  -p, --province PROVINCE    省份 (默认: jiangsu)"
            echo "  -m, --monthid MONTHID      月份ID (默认: 202306)"
            echo "  --only-graph               只运行图计算"
            echo "  --only-tv                  只运行特征计算"
            echo "  -h, --help                 显示帮助信息"
            echo ""
            echo "示例:"
            echo "  ./run_detection.sh"
            echo "  ./run_detection.sh -p shandong -m 202308"
            echo "  ./run_detection.sh --only-graph"
            echo ""
            echo "首次运行会自动："
            echo "  - 创建虚拟环境"
            echo "  - 安装依赖包 (pyspark, networkx, PyYAML)"
            echo "  - 配置 Java 17"
            exit 0
            ;;
        *)
            echo "未知参数: $1"
            echo "使用 -h 或 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 显示运行参数
echo "运行参数:"
echo "  省份: ${PROVINCE}"
echo "  月份: ${MONTHID}"
echo "  模式: ${MODE}"
echo "  只运行图计算: $([ "$ONLY_GRAPH" = "1" ] && echo "是" || echo "否")"
echo "  只运行特征计算: $([ "$ONLY_TV" = "1" ] && echo "是" || echo "否")"
echo ""
echo "日志文件: logs/detection_${PROVINCE}_${MONTHID}.out"
echo ""
echo "开始运行..."
echo ""

# ============================================
# 运行检测程序
# ============================================

python3 main.py \
    --province ${PROVINCE} \
    --monthid ${MONTHID} \
    --mode ${MODE} \
    --model_type ${MODEL_TYPE} \
    --load_graph_model 0 \
    --only_graph ${ONLY_GRAPH} \
    --only_tv ${ONLY_TV} \
    --load_graph_result 0 \
    --load_tv_result 0

EXIT_CODE=$?

# ============================================
# 显示结果
# ============================================

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "=========================================="
    echo "✅ 运行成功！"
    echo "=========================================="
    echo ""
    echo "结果文件："

    if [ "$ONLY_GRAPH" = "1" ]; then
        echo "  - results/${PROVINCE}/graph_result_${PROVINCE}_${MONTHID}.csv"
    elif [ "$ONLY_TV" = "1" ]; then
        echo "  - results/${PROVINCE}/tv_result_${PROVINCE}_${MONTHID}.csv"
    else
        echo "  - results/${PROVINCE}/unreal_person_${PROVINCE}_${MONTHID}.csv"
    fi

    echo ""
    echo "中间结果:"
    echo "  - results/inter/${PROVINCE}/"
    echo ""
    echo "模型文件:"
    echo "  - save_models/graph_nx_${PROVINCE}_${MONTHID}.pkl"
    echo ""
else
    echo "=========================================="
    echo "❌ 运行失败 (退出码: ${EXIT_CODE})"
    echo "=========================================="
    echo ""
    echo "请查看日志文件:"
    echo "  cat logs/detection_${PROVINCE}_${MONTHID}.out"
    echo ""
fi

exit $EXIT_CODE
