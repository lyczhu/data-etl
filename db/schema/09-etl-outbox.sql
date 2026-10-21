-- ============================================================================
-- 09-etl-outbox.sql
-- Phase 2C：派工事务性 Outbox。
--
-- 顺序：markAssigned CAS 成功 -> 同一事务插入 outbox -> 提交；poller 异步发 Kafka。
-- 进程在提交前崩溃：分片保持 PENDING（CAS 回滚），无消息；
-- 进程在提交后、发送前崩溃：outbox 行仍在，poller 重发（worker 以 idempotency_key 去重）。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_outbox (
    id              BIGSERIAL PRIMARY KEY,
    topic           VARCHAR(128) NOT NULL,
    message_key     VARCHAR(128),
    payload         TEXT         NOT NULL,
    attempt_no      INT          NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error      VARCHAR(512),
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at         TIMESTAMP
);

COMMENT ON TABLE etl_outbox IS '派工 Outbox：与 markAssigned 同事务写入，poller 异步投递 Kafka';
COMMENT ON COLUMN etl_outbox.next_attempt_at IS '下次可投递时间（失败指数退避）';
COMMENT ON COLUMN etl_outbox.sent_at IS '投递成功时间；NULL 表示待投递';

-- 只索引未发送行，避免已发送历史撑大索引
CREATE INDEX IF NOT EXISTS idx_outbox_due ON etl_outbox (id) WHERE sent_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_sent_at ON etl_outbox (sent_at) WHERE sent_at IS NOT NULL;
