#!/bin/bash

# 设置工作目录
cd "$(dirname "$0")"

# 读取服务配置文件
echo "正在加载服务配置文件..."
if [ ! -f "service.conf" ]; then
    echo "错误: 找不到 service.conf 配置文件"
    exit 1
fi

# 解析配置文件
while IFS='=' read -r key value; do
    # 去除空格和注释
    key=$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    
    # 跳过注释和空行
    [[ "$key" =~ ^#.*$ ]] && continue
    [[ -z "$key" ]] && continue
    
    case "$key" in
        SERVICE_NAME) SERVICE_NAME="$value" ;;
        JAVA_HEAP_MIN) JAVA_HEAP_MIN="$value" ;;
        JAVA_HEAP_MAX) JAVA_HEAP_MAX="$value" ;;
        JAVA_METASPACE_MIN) JAVA_METASPACE_MIN="$value" ;;
        JAVA_METASPACE_MAX) JAVA_METASPACE_MAX="$value" ;;
        JAVA_GC_TYPE) JAVA_GC_TYPE="$value" ;;
        JAVA_OPTS) JAVA_OPTS="$value" ;;
        SPRING_OPTS) SPRING_OPTS="$value" ;;
        SERVER_PORT) SERVER_PORT="$value" ;;
    esac
done < "service.conf"

echo "============================================"
echo "服务名称: $SERVICE_NAME"
echo "服务端口: $SERVER_PORT"
echo "============================================"

# 检查Java环境
if ! command -v java &> /dev/null; then
    echo "错误: 未找到Java运行环境，请安装JDK 17或更高版本"
    exit 1
fi

# 创建日志目录
mkdir -p logs

# 查找JAR文件
JAR_FILE=$(find lib -name "*.jar" -not -name "*-sources.jar" -not -name "*-javadoc.jar" 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "错误: 在lib目录下未找到JAR文件"
    echo "请确保已正确解压分发包"
    exit 1
fi

echo "启动JAR文件: $JAR_FILE"

# 构建JVM参数
JVM_ARGS="-Xms${JAVA_HEAP_MIN} -Xmx${JAVA_HEAP_MAX}"
JVM_ARGS="$JVM_ARGS -XX:MetaspaceSize=${JAVA_METASPACE_MIN} -XX:MaxMetaspaceSize=${JAVA_METASPACE_MAX}"
JVM_ARGS="$JVM_ARGS -XX:+Use${JAVA_GC_TYPE}"
JVM_ARGS="$JVM_ARGS ${JAVA_OPTS}"

# 启动服务
echo ""
echo "正在启动 $SERVICE_NAME ..."
echo "JVM参数: $JVM_ARGS"
echo ""

java $JVM_ARGS -jar "$JAR_FILE" $SPRING_OPTS

# 如果服务异常退出
echo ""
echo "服务已停止，退出代码: $?"
