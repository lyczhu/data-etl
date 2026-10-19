@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

:: 设置工作目录
cd /d "%~dp0"

:: 默认配置值
set "SERVICE_NAME=task-management-service"
set "JAVA_HEAP_MIN=1024m"
set "JAVA_HEAP_MAX=1024m"
set "JAVA_METASPACE_MIN=128m"
set "JAVA_METASPACE_MAX=256m"
set "JAVA_GC_TYPE=G1GC"
set "JAVA_OPTS=-server -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai"
set "SERVER_PORT=8081"

:: 读取服务配置文件
echo 正在加载服务配置文件...
if exist "service.conf" (
    for /f "usebackq eol=# tokens=1,2 delims==" %%A in ("service.conf") do (
        set "key=%%A"
        set "value=%%B"
        :: 去除前后空格
        for /f "tokens=*" %%i in ("!key!") do set "key=%%i"
        for /f "tokens=*" %%i in ("!value!") do set "value=%%i"
        
        if "!key!"=="SERVICE_NAME" set "SERVICE_NAME=!value!"
        if "!key!"=="JAVA_HEAP_MIN" set "JAVA_HEAP_MIN=!value!"
        if "!key!"=="JAVA_HEAP_MAX" set "JAVA_HEAP_MAX=!value!"
        if "!key!"=="JAVA_METASPACE_MIN" set "JAVA_METASPACE_MIN=!value!"
        if "!key!"=="JAVA_METASPACE_MAX" set "JAVA_METASPACE_MAX=!value!"
        if "!key!"=="JAVA_GC_TYPE" set "JAVA_GC_TYPE=!value!"
        if "!key!"=="JAVA_OPTS" set "JAVA_OPTS=!value!"
        if "!key!"=="SPRING_OPTS" set "SPRING_OPTS=!value!"
        if "!key!"=="SERVER_PORT" set "SERVER_PORT=!value!"
    )
) else (
    echo 警告: 找不到 service.conf 配置文件，使用默认配置
)

echo ============================================
echo 服务名称: %SERVICE_NAME%
echo 服务端口: %SERVER_PORT%
echo ============================================

:: 检查Java环境
java -version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Java运行环境，请安装JDK 17或更高版本
    exit /b 1
)

:: 创建日志目录
if not exist "logs" mkdir logs

:: 查找JAR文件
set "JAR_FILE="
for %%f in (lib\*.jar) do (
    set "JAR_FILE=%%f"
)

if not defined JAR_FILE (
    echo 错误: 在lib目录下未找到JAR文件
    echo 请确保已正确解压分发包
    exit /b 1
)

echo 启动JAR文件: %JAR_FILE%

:: 构建JVM参数
set "JVM_ARGS=-Xms%JAVA_HEAP_MIN% -Xmx%JAVA_HEAP_MAX%"
set "JVM_ARGS=%JVM_ARGS% -XX:MetaspaceSize=%JAVA_METASPACE_MIN% -XX:MaxMetaspaceSize=%JAVA_METASPACE_MAX%"
set "JVM_ARGS=%JVM_ARGS% -XX:+Use%JAVA_GC_TYPE%"
set "JVM_ARGS=%JVM_ARGS% %JAVA_OPTS%"

:: 启动服务
echo.
echo 正在启动 %SERVICE_NAME% ...
echo JVM参数: %JVM_ARGS%
echo.

java %JVM_ARGS% -jar "%JAR_FILE%" %SPRING_OPTS%

:: 如果服务异常退出
echo.
echo 服务已停止，退出代码: %errorlevel%
pause
