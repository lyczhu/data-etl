package com.lawyus.dataetl.connector.spi.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcSourceCursorTest {

    @Test
    @DisplayName("resumeCursor 解析：空/非数字回落首跑（null），数字按开区间续跑")
    void parseResumeCursor() {
        assertThat(JdbcSourceConnector.parseCursor(null)).isNull();
        assertThat(JdbcSourceConnector.parseCursor("")).isNull();
        assertThat(JdbcSourceConnector.parseCursor("  ")).isNull();
        assertThat(JdbcSourceConnector.parseCursor("not-a-number")).isNull();
        assertThat(JdbcSourceConnector.parseCursor("42")).isEqualTo(42L);
        assertThat(JdbcSourceConnector.parseCursor(" 42 ")).isEqualTo(42L);
    }
}
