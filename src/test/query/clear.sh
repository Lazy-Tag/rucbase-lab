#!/bin/bash

port=8765

echo "正在查找端口号 $port 对应的进程..."
pid=$(lsof -t -i:$port)

if [ -n "$pid" ]; then
    echo "找到端口号 $port 对应的进程PID：$pid"
    sudo kill -9 "$pid"
    echo "进程已结束。"
else
    echo "没有找到使用端口号 $port 的进程。"
fi
