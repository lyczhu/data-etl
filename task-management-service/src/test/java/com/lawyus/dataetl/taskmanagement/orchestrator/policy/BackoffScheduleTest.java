package com.lawyus.dataetl.taskmanagement.orchestrator.policy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffScheduleTest {

    private final List<Duration> backoff = List.of(
            Duration.ofMinutes(5), Duration.ofMinutes(15), Duration.ofHours(1));
    private final Date now = new Date(1_700_000_000_000L);

    @Test
    @DisplayName("attempt 逐档：1→5m、2→15m、3→1h、越界钳到最后一档")
    void picksTierByAttempt() {
        assertThat(BackoffSchedule.nextAttemptAt(backoff, 1, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofMinutes(5).toMillis());
        assertThat(BackoffSchedule.nextAttemptAt(backoff, 2, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofMinutes(15).toMillis());
        assertThat(BackoffSchedule.nextAttemptAt(backoff, 3, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofHours(1).toMillis());
        assertThat(BackoffSchedule.nextAttemptAt(backoff, 9, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofHours(1).toMillis());
    }

    @Test
    @DisplayName("attempt<=0 归入首档；空退避序列退化 1 分钟")
    void edgeCases() {
        assertThat(BackoffSchedule.nextAttemptAt(backoff, 0, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofMinutes(5).toMillis());
        assertThat(BackoffSchedule.nextAttemptAt(List.of(), 1, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofMinutes(1).toMillis());
        assertThat(BackoffSchedule.nextAttemptAt(null, 2, now).getTime() - now.getTime())
                .isEqualTo(Duration.ofMinutes(1).toMillis());
    }
}
