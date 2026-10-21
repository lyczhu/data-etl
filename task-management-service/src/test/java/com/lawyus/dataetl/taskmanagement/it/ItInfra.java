package com.lawyus.dataetl.taskmanagement.it;

import org.junit.jupiter.api.Assumptions;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * §12.1 集成测试的基础设施门面：探测 docker-compose 提供的真实 PG/Kafka/Redis，自建 IT 专用库
 * {@code etl_it} 并按 {@code db/schema} 里的权威 DDL（02/05/06，含 Hibernate 建不出的<b>部分唯一索引</b>
 * 与 {@code CHECK} 约束）幂等落库，最后准备一张可被 JDBC connector 真实读取的源表。
 *
 * <p>设计取向（诚实边界）：
 * <ul>
 *   <li>IT 跑在<b>独立库 etl_it</b> 上，绝不污染现网 e2e 用的 {@code etl_system}；</li>
 *   <li>topic 名带"每次 JVM 运行唯一"的标签，避免与历史残留/现网链路互相消费；</li>
 *   <li>基础设施不可用时 {@link #assumeInfrastructureUp()} 让用例<b>跳过</b>而非失败——
 *       IT 由 {@code -Pit} 显式开启，未起 docker 时不该把构建染红。</li>
 * </ul>
 */
public final class ItInfra {

    /** docker-compose 里的 postgres 宿主端口（见根目录 docker-compose.yml）。 */
    public static final String PG_HOST = "localhost";
    public static final int PG_PORT = 5433;
    public static final String PG_USER = "postgres";
    public static final String PG_PASSWORD = "postgres";
    /** IT 专用库；{@code postgres} 库只用来建库。 */
    public static final String IT_DB = "etl_it";
    public static final String IT_DB_URL = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + IT_DB;
    private static final String MAINT_DB_URL = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/postgres";

    public static final String KAFKA_HOST = "localhost";
    public static final int KAFKA_PORT = 9092;
    public static final String KAFKA_BOOTSTRAP = KAFKA_HOST + ":" + KAFKA_PORT;
    public static final String REDIS_HOST = "localhost";
    public static final int REDIS_PORT = 6379;

    /** Phase 2A：MySQL 8.0 回归实例（docker compose --profile mysql up -d mysql，使用本地镜像）。 */
    public static final String MYSQL_HOST = "localhost";
    public static final int MYSQL_PORT = 3307;
    public static final String MYSQL_USER = "etl";
    public static final String MYSQL_PASSWORD = "etl";
    public static final String MYSQL_DB = "etl_it_mysql";
    public static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=UTF-8";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    /** IT 源表（真 JDBC 读取目标）：20 行整型主键 1..20，含数值/文本/时间戳列。 */
    public static final String SOURCE_TABLE = "it_source_order";
    public static final int SOURCE_ROWS = 20;
    /** 源表连接信息：orchestrator 侧 Feign 被 stub 成这条记录，worker 侧则真连它。 */
    public static final String SOURCE_URL = IT_DB_URL;
    public static final String SOURCE_DRIVER = "org.postgresql.Driver";
    public static final String SOURCE_TYPE = "postgresql";

    /** 每次 JVM 运行唯一的 topic 前缀，杜绝跨运行/跨链路串扰。 */
    private static final String RUN_TAG = "it" + Long.toString(Math.abs(System.nanoTime()), 36);
    public static final String TOPIC_DISPATCH = RUN_TAG + ".etl.task.dispatch.batch";
    public static final String TOPIC_PROGRESS = RUN_TAG + ".etl.task.progress";
    public static final String TOPIC_RESULTS = RUN_TAG + ".etl.task.results";
    public static final String TOPIC_DLT = RUN_TAG + ".etl.task.dlt";
    public static final String[] IT_TOPICS = {TOPIC_DISPATCH, TOPIC_PROGRESS, TOPIC_RESULTS, TOPIC_DLT};
    /** dispatch 分区数：Gate1 #9（1 任务 8 分片、2 worker）需要同组消费者能分担多个分区。 */
    public static final int DISPATCH_PARTITIONS = 8;
    /**
     * IT worker 的消费组（每次 JVM 唯一）。worker 的 {@code @KafkaListener} 从
     * {@code etl.worker.consumer-group} 读取；不能沿用生产默认 {@code etl-worker}，否则会与本地
     * 正在运行的 e2e worker 混组——两者订阅不同 topic，group 会无限 rebalance。
     */
    public static final String WORKER_GROUP = RUN_TAG + "-etl-worker";

    private static final String[] SCHEMA_SCRIPTS = {
            "00-extensions.sql", "02-create-tables.sql", "05-etl-task-run.sql",
            "06-etl-task-checkpoint.sql", "07-etl-task-dlt.sql", "08-etl-task-audit.sql",
            "09-etl-outbox.sql", "10-etl-reconciliation.sql"};

    private static final AtomicBoolean BOOTSTRAPPED = new AtomicBoolean(false);

    private ItInfra() {
    }

    /**
     * 基础设施不在 → 用例跳过（而不是失败）。每个 IT 类在任何 DB/Kafka 操作前调用一次，
     * 通过后紧接着 {@link #bootstrapOnce()}。
     */
    public static void assumeInfrastructureUpAndBootstrap() {
        Assumptions.assumeTrue(tcpOpen(PG_HOST, PG_PORT),
                "跳过：PostgreSQL 未就绪，" + PG_HOST + ":" + PG_PORT + " 不可达（docker compose up -d postgres）");
        Assumptions.assumeTrue(tcpOpen(KAFKA_HOST, KAFKA_PORT),
                "跳过：Kafka 未就绪，" + KAFKA_BOOTSTRAP + " 不可达（docker compose up -d kafka）");
        Assumptions.assumeTrue(tcpOpen(REDIS_HOST, REDIS_PORT),
                "跳过：Redis 未就绪，" + REDIS_HOST + ":" + REDIS_PORT + " 不可达（docker compose up -d redis）");
        bootstrapOnce();
    }

    /** MySQL 回归实例是否就绪；未启动时相关 IT 跳过而非失败。 */
    public static void assumeMySqlUp() {
        Assumptions.assumeTrue(tcpOpen(MYSQL_HOST, MYSQL_PORT),
                "跳过：MySQL 未就绪，" + MYSQL_HOST + ":" + MYSQL_PORT
                        + " 不可达（docker compose --profile mysql up -d mysql）");
    }

    /** 建库 + 落 DDL + 清残数据 + 重建源表；同一 JVM 只做一次。 */
    private static void bootstrapOnce() {
        if (BOOTSTRAPPED.get()) {
            return;
        }
        synchronized (ItInfra.class) {
            if (BOOTSTRAPPED.get()) {
                return;
            }
            createDatabaseIfAbsent();
            applySchemaScripts();
            resetData();
            BOOTSTRAPPED.set(true);
        }
    }

    /**
     * 显式建出本 JVM 专用的 IT topic，不依赖 broker 的 auto-create 与默认分区数。
     * dispatch topic 用多分区：Gate1 #9 要在一个任务 8 分片下让 2 个 worker 真正分担消费；
     * 同一 shard 的消息仍按 {@code runId:shardId} key 保序。其它 topic 保持 1 分区（顺序/位点断言简单）。
     * 幂等：已存在则忽略。
     */
    public static void ensureItTopics() {
        java.util.Properties props = new java.util.Properties();
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_ID_CONFIG, "it-topic-init");
        try (org.apache.kafka.clients.admin.AdminClient admin =
                     org.apache.kafka.clients.admin.AdminClient.create(props)) {
            List<org.apache.kafka.clients.admin.NewTopic> topics = new ArrayList<>();
            for (String topic : IT_TOPICS) {
                int partitions = topic.equals(TOPIC_DISPATCH) ? DISPATCH_PARTITIONS : 1;
                topics.add(new org.apache.kafka.clients.admin.NewTopic(topic, partitions, (short) 1));
            }
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (org.apache.kafka.common.errors.TopicExistsException exists) {
            // 同一 JVM 内重复调用（多个 IT 类）——正常
        } catch (Exception e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                return;
            }
            throw new IllegalStateException("创建 IT topic 失败（" + KAFKA_BOOTSTRAP + "）", e);
        }
    }

    private static void createDatabaseIfAbsent() {
        try (Connection conn = DriverManager.getConnection(MAINT_DB_URL, props())) {
            conn.setAutoCommit(true);
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery(
                        "SELECT 1 FROM pg_database WHERE datname = '" + IT_DB + "'")) {
                    if (!rs.next()) {
                        st.execute("CREATE DATABASE " + IT_DB + " ENCODING 'UTF8'");
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("无法创建 IT 库 " + IT_DB + "（" + MAINT_DB_URL + "）", e);
        }
    }

    /** 权威 DDL 直接复用 db/schema 下的脚本，避免测试里复制一份产生漂移。 */
    private static void applySchemaScripts() {
        try (Connection conn = DriverManager.getConnection(IT_DB_URL, props())) {
            conn.setAutoCommit(true);
            for (String script : SCHEMA_SCRIPTS) {
                ScriptUtils.executeSqlScript(conn,
                        new EncodedResource(new FileSystemResource(resolveSchemaFile(script)),
                                StandardCharsets.UTF_8));
            }
        } catch (SQLException | IOException e) {
            throw new IllegalStateException("IT 库 DDL 初始化失败（db/schema 02/05/06/07）", e);
        }
    }

    /**
     * 清掉上一轮 IT 留下的数据：删除所有 {@code it_} 前缀表（含 worker 建的动态目标表）、
     * 截断三张编排表，然后重建并灌入源表。
     */
    private static void resetData() {
        try (Connection conn = DriverManager.getConnection(IT_DB_URL, props())) {
            conn.setAutoCommit(true);
            List<String> itTables = new ArrayList<>();
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery(
                        "SELECT table_name FROM information_schema.tables "
                                + "WHERE table_schema = 'public' AND table_name LIKE 'it\\_%'")) {
                    while (rs.next()) {
                        itTables.add(rs.getString(1));
                    }
                }
                for (String table : itTables) {
                    st.execute("DROP TABLE IF EXISTS \"" + table + "\" CASCADE");
                }
                st.execute("TRUNCATE TABLE data_source, etl_task, etl_task_run, etl_task_checkpoint, "
                        + "etl_task_dlt, etl_task_audit, etl_outbox, etl_reconciliation_run "
                        + "RESTART IDENTITY CASCADE");
            }
            seedSourceTable(conn);
        } catch (SQLException e) {
            throw new IllegalStateException("IT 数据复位失败", e);
        }
    }

    private static void seedSourceTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(64), "
                    + "amount NUMERIC(10, 2), "
                    + "note VARCHAR(120), "
                    + "created_at TIMESTAMP)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + SOURCE_TABLE
                + " (id, name, amount, note, created_at) VALUES (?, ?, ?, ?, ?)")) {
            for (int i = 1; i <= SOURCE_ROWS; i++) {
                ps.setLong(1, i);
                ps.setString(2, "order-" + i);
                ps.setBigDecimal(3, new BigDecimal("10.50"));
                ps.setString(4, "it fixture row " + i);
                ps.setTimestamp(5, Timestamp.valueOf("2026-01-01 00:00:00"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** {@code mvn test} 的工作目录是模块根，IDE 里可能是仓库根——两个位置都试。 */
    private static Path resolveSchemaFile(String script) throws IOException {
        String[][] candidates = {
                {"..", "db", "schema", script},
                {"db", "schema", script},
        };
        for (String[] parts : candidates) {
            Path path = Path.of(String.join(File.separator, parts)).toAbsolutePath().normalize();
            if (Files.exists(path)) {
                return path;
            }
        }
        throw new IOException("找不到 IT 所需的 DDL 脚本 db/schema/" + script
                + "（工作目录：" + Path.of("").toAbsolutePath() + "）");
    }

    private static Properties props() {
        Properties p = new Properties();
        p.setProperty("user", PG_USER);
        p.setProperty("password", PG_PASSWORD);
        return p;
    }

    public static boolean tcpOpen(String host, int port) {
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
