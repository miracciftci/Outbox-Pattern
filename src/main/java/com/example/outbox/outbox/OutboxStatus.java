package com.example.outbox.outbox;

public enum OutboxStatus {
    /** Yazıldı, henüz broker'a gönderilmedi. */
    PENDING,
    /** Broker onayı (ack) alındı. */
    PROCESSED,
    /** Retry eşiği aşıldı; alarm üretilmeli, elle incelenmeli. */
    FAILED
}
