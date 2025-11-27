#!/bin/bash

set -e  # 遇到错误立即退出

TARGET_DIRS=("store" "run" "etc")

echo "开始删除以下目录："
printf "  - %s\n" "${TARGET_DIRS[@]}"

for DIR in "${TARGET_DIRS[@]}"; do
  if [ -d "$DIR" ]; then
    echo "删除目录: $DIR"
    rm -rf "$DIR"
  else
    echo "目录不存在: $DIR (跳过)"
  fi
done

echo "完成！"