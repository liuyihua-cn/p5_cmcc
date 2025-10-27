# 实名不实人检测系统 - 核心版

## 🎯 这是什么？

这是**实名不实人检测系统的精简核心版本**，只包含运行必需的 13 个文件。

- **原项目**: 510 MB
- **核心版本**: 2 MB
- **精简比例**: 99.6%

## 🚀 一键运行

```bash
./run_detection.sh
```

**就这么简单！** 脚本会自动：
- ✅ 检查并创建虚拟环境
- ✅ 检查并安装依赖包
- ✅ 配置 Java 环境
- ✅ 运行检测程序

## 📋 前置条件

只需要确保系统已安装：
- **Python 3.13+** (macOS 自带或 `brew install python`)
- **Java 17** (会自动检查，如未安装：`brew install openjdk@17`)

**仅此而已！**其他一切都会自动处理。

## 📂 项目结构

```
UnrealPerson_Core/
├── run_detection.sh          # 智能启动脚本（只需运行这个）
├── config.yaml               # 配置文件
├── main.py                   # 主程序
├── data/                     # 数据文件（3个）
├── data_process/             # 数据处理模块（2个.py）
├── model/                    # 模型模块（3个.py）
└── utils/                    # 工具模块（2个.py）
```

**总共 13 个核心文件，就能完整运行！**

## 💡 使用示例

### 基本用法
```bash
# 运行完整检测（图计算 + 特征计算）
./run_detection.sh

# 只运行图计算（更快）
./run_detection.sh --only-graph

# 只运行特征计算
./run_detection.sh --only-tv
```

### 指定参数
```bash
# 指定省份和月份
./run_detection.sh -p shandong -m 202308

# 查看帮助
./run_detection.sh --help
```

## 📊 查看结果

运行成功后，结果保存在：
```bash
results/jiangsu/graph_result_jiangsu_202306.csv/
```

查看结果：
```bash
cat results/jiangsu/graph_result_jiangsu_202306.csv/*.csv
```

## 🔧 智能特性

脚本会智能检查环境：

1. **虚拟环境**
   - 不存在？→ 自动创建
   - 未激活？→ 自动激活
   - 已激活？→ 继续运行

2. **依赖包**
   - 检查 pyspark、networkx、PyYAML
   - 缺失？→ 自动安装
   - 已安装？→ 显示版本

3. **版本固定**
   - PySpark: 4.0.1（支持 Python 3.13）
   - NetworkX: 最新稳定版
   - PyYAML: 最新稳定版

## 📈 运行流程

```
./run_detection.sh
  ↓
✓ 检查 Python 3.13
✓ 检查 Java 17
✓ 检查/创建虚拟环境
✓ 激活虚拟环境
✓ 检查/安装依赖包
✓ 运行检测程序
✓ 生成结果
```

## 🎯 输出结果

### 图计算结果字段
- `USER_ID` - 用户ID
- `1_HOP_NEI_COUNT` - 1跳邻居数
- `CALL_OTHER_USER_COUNT` - 通话其他用户数
- `1_HOP_CONNECT_NEI_COUNT` - 1跳连通邻居数
- `USERS_COMMON_NEI_COUNT` - 共同邻居数
- `USERS_1_HOP_NEI_CONNECT_COUNT` - 1跳邻居连接数
- `STATIS_YM` - 统计年月

## ⚙️ 配置说明

配置文件 `config.yaml` 中的关键设置：
- `mode: "local"` - 本地模式
- `model_type: "nx"` - 使用 NetworkX（纯 Python）
- `data/` - 数据目录（call.txt, user.txt, tv.txt）

## ❓ 常见问题

### 首次运行慢？
正常，需要：
- 创建虚拟环境（~5秒）
- 安装依赖包（~60秒）
- 后续运行会很快！

### 遇到错误？
```bash
# 查看日志
cat logs/detection_jiangsu_202306.out

# 重新安装环境
rm -rf venv
./run_detection.sh
```

### 切换数据？
替换 `data/` 目录下的三个文件：
- `call.txt` - 通话数据
- `user.txt` - 用户数据
- `tv.txt` - 特征数据

## 🌟 核心优势

1. **极简设计** - 只有 13 个核心文件
2. **智能运行** - 自动处理所有环境配置
3. **零依赖困扰** - 版本固定，兼容性好
4. **开箱即用** - 一个命令搞定一切

## 📞 需要帮助？

运行遇到问题？
1. 检查 Python 和 Java 是否安装
2. 查看日志文件
3. 使用 `./run_detection.sh --help` 查看帮助

---

**就是这么简单！享受吧！** 🎊
