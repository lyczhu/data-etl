package com.lawyus.dataetl.worker.pipeline;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

/**
 * 错误分类（方案 v2 WP-05 / D8）：替换“任何异常都标 PERSISTENT”的盲重试。
 *
 * <ul>
 *   <li>{@link Kind#TRANSIENT}：连接/锁/序列化冲突/超时，可退避重试；</li>
 *   <li>{@link Kind#PERSISTENT}：一般运行时错误，分片内重试有限次；</li>
 *   <li>{@link Kind#FATAL}：约束/数据/语法错误，立即 DLT，不重试。</li>
 * </ul>
 */
public final class ErrorClassifier {

    public enum Kind {
        TRANSIENT, PERSISTENT, FATAL
    }

    private ErrorClassifier() {
    }

    public static Kind classify(Throwable e) {
        Throwable t = root(e);
        if (t instanceof SQLException se) {
            String state = se.getSQLState();
            if (state == null) {
                return Kind.PERSISTENT;
            }
            // 08*=连接异常，40*=事务回滚/序列化失败，55P03=锁不可用，40001/40P01=死锁
            if (state.startsWith("08") || state.startsWith("40")
                    || "55P03".equals(state) || "40001".equals(state) || "40P01".equals(state)) {
                return Kind.TRANSIENT;
            }
            // 22*=数据异常，23*=完整性约束，42*=语法/权限：重试无意义
            if (state.startsWith("22") || state.startsWith("23") || state.startsWith("42")) {
                return Kind.FATAL;
            }
            return Kind.PERSISTENT;
        }
        if (t instanceof SocketTimeoutException || t instanceof TimeoutException
                || t instanceof IOException) {
            return Kind.TRANSIENT;
        }
        if (t instanceof IllegalArgumentException || t instanceof UnsupportedOperationException) {
            return Kind.FATAL;
        }
        return Kind.PERSISTENT;
    }

    private static Throwable root(Throwable e) {
        Throwable cur = e;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }
}
