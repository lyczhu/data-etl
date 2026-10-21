package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import java.sql.Timestamp;

/** 一条待投递的 outbox 行。 */
public record OutboxRow(long id,
                        String topic,
                        String messageKey,
                        String payload,
                        int attemptNo,
                        Timestamp createdAt) {
}
