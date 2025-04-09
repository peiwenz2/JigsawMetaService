#!/bin/bash

# Redis集群配置
HOST="172.30.0.72"
PORT=7003

# 默认参数
KEY_START=1
KEY_END=100
PRINT_INTERVAL=1000
VALUE_TYPE=""
PREFIXES=()

show_usage() {
    echo "Usage: $0 [options] <value_type> [prefixes...]"
    echo "参数说明:"
    echo "  -s, --start N     起始键编号（默认: $KEY_START）"
    echo "  -e, --end N       结束键编号（默认: $KEY_END）"
    echo "  -i, --interval N  进度打印间隔（默认: $PRINT_INTERVAL）"
    echo "  value_type        数据类型：int/string"
    echo "  prefixes          键前缀（可多个，默认: instance）"
    echo "示例:"
    echo "  $0 -s 1 -e 200000 int instance"
    echo "  $0 string -e 5000 user product"
    echo "  $0 -i 500 int -s 10 -e 100 user"
}

# 使用getopts解析选项参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--start)
            KEY_START="$2"
            shift 2
            ;;
        -e|--end)
            KEY_END="$2"
            shift 2
            ;;
        -i|--interval)
            PRINT_INTERVAL="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        int|string)
            VALUE_TYPE="$1"
            shift
            # 收集剩余参数作为前缀
            while [[ $# -gt 0 && "$1" != -* ]]; do
                PREFIXES+=("$1")
                shift
            done
            ;;
        *)
            echo "错误：未知参数 $1"
            show_usage
            exit 1
            ;;
    esac
done

# 设置默认前缀
if [ ${#PREFIXES[@]} -eq 0 ]; then
    PREFIXES=("instance")
fi

# 参数校验
validate_args() {
    if [ -z "$VALUE_TYPE" ]; then
        echo "必须指定数据类型（int/string）"
        show_usage
        exit 1
    fi

    if ! [[ "$KEY_START" =~ ^[0-9]+$ ]]; then
        echo "错误：起始键必须是数字"
        exit 1
    fi

    if ! [[ "$KEY_END" =~ ^[0-9]+$ ]]; then
        echo "错误：结束键必须是数字"
        exit 1
    fi

    if (( KEY_START > KEY_END )); then
        echo "错误：起始键不能大于结束键"
        exit 1
    fi
}

# 生成随机字符串（256字节）
generate_string() {
    openssl rand -base64 192 | tr -d '\n'
}

# 主程序
main() {
    validate_args

    total_keys=0
    start_time=$(date +%s)

    for prefix in "${PREFIXES[@]}"; do
        echo "[$(date +'%T')] 开始处理前缀: $prefix (范围: $KEY_START-$KEY_END)"
        
        command_buffer=""
        buffer_size=0
        max_buffer=1000

        for ((i=KEY_START; i<=KEY_END; i++)); do
            key="${prefix}-${i}"
            
            if [ "$VALUE_TYPE" == "int" ]; then
                value=$(( RANDOM % 1001 ))
            else
                value=$(generate_string)
            fi

            command_buffer+="SET $key $value"$'\n'
            ((buffer_size++))

            if (( buffer_size >= max_buffer )); then
                echo -n "$command_buffer" | \
                    redis-cli -c -h "$HOST" -p "$PORT" --pipe >/dev/null 2>&1
                command_buffer=""
                buffer_size=0
            fi

            ((total_keys++))
            if (( total_keys % PRINT_INTERVAL == 0 )); then
                echo "[$(date +'%T')] 已写入 $total_keys 个键..."
            fi
        done

        if [ -n "$command_buffer" ]; then
            echo -n "$command_buffer" | \
                redis-cli -c -h "$HOST" -p "$PORT" --pipe >/dev/null 2>&1
        fi
    done

    end_time=$(date +%s)
    duration=$((end_time - start_time))

    echo "========================================"
    echo "总计写入键数: $total_keys"
    printf "耗时: %02d分%02d秒\n" $((duration/60)) $((duration%60))
    echo "平均速率: $(( total_keys / (duration > 0 ? duration : 1) )) 键/秒"
}

main