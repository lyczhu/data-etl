package com.lawyus.dataetl.worker.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorClassifierTest {

    @Test
    @DisplayName("连接类 SQLState 08* → TRANSIENT")
    void connectionFailureIsTransient() {
        assertThat(ErrorClassifier.classify(new SQLException("reset", "08006")))
                .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    @DisplayName("序列化/死锁 40* → TRANSIENT")
    void serializationFailureIsTransient() {
        assertThat(ErrorClassifier.classify(new SQLException("deadlock", "40P01")))
                .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    @DisplayName("约束/数据/语法 22*/23*/42* → FATAL，不重试")
    void dataAndConstraintFailuresAreFatal() {
        assertThat(ErrorClassifier.classify(new SQLException("bad value", "22001")))
                .isEqualTo(ErrorClassifier.Kind.FATAL);
        assertThat(ErrorClassifier.classify(new SQLException("duplicate", "23505")))
                .isEqualTo(ErrorClassifier.Kind.FATAL);
        assertThat(ErrorClassifier.classify(new SQLException("syntax", "42601")))
                .isEqualTo(ErrorClassifier.Kind.FATAL);
    }

    @Test
    @DisplayName("无 SQLState 的 SQLException → PERSISTENT（有限重试）")
    void sqlExceptionWithoutStateIsPersistent() {
        assertThat(ErrorClassifier.classify(new SQLException("unknown")))
                .isEqualTo(ErrorClassifier.Kind.PERSISTENT);
    }

    @Test
    @DisplayName("网络超时/IO → TRANSIENT")
    void ioFailuresAreTransient() {
        assertThat(ErrorClassifier.classify(new SocketTimeoutException("timeout")))
                .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
        assertThat(ErrorClassifier.classify(new IOException("broken pipe")))
                .isEqualTo(ErrorClassifier.Kind.TRANSIENT);
    }

    @Test
    @DisplayName("参数/不支持操作 → FATAL")
    void programmerErrorsAreFatal() {
        assertThat(ErrorClassifier.classify(new IllegalArgumentException("bad")))
                .isEqualTo(ErrorClassifier.Kind.FATAL);
        assertThat(ErrorClassifier.classify(new UnsupportedOperationException("no")))
                .isEqualTo(ErrorClassifier.Kind.FATAL);
    }

    @Test
    @DisplayName("按 root cause 分类，而不是外层包装类型")
    void classifiesRootCause() {
        RuntimeException wrapped = new RuntimeException("write failed",
                new SQLException("connection reset", "08006"));
        assertThat(ErrorClassifier.classify(wrapped)).isEqualTo(ErrorClassifier.Kind.TRANSIENT);
    }
}
