#!/bin/bash
set -e

echo "=== 1. 杀掉 observer 进程 ==="
OB_PID=$(pidof observer)
kill -9 $OB_PID

echo ""
echo "=== 2. 开始 debug 构建 ==="
bash build.sh debug -DOB_USE_CCACHE=ON --init --make -j8

echo ""
echo "=== 3. 拷贝 observer 到 ~/seekdb/bin（自动覆盖） ==="
# 使用 yes 自动输入 y 覆盖文件
yes | cp -f build_debug/src/observer/observer ~/seekdb/bin/

./bin/observer

echo "=== 重启流程完成 ==="