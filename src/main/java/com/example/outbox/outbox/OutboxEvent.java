package com.example.outbox.outbox;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    private String id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Lob
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error")
    private String lastError;

    protected OutboxEvent() {
    }

    public static OutboxEvent create(String eventId, String aggregateType, String aggregateId,
                                     String eventType, String payload) {
        OutboxEvent e = new OutboxEvent();
        e.id = eventId;
        e.aggregateType = aggregateType;
        e.aggregateId = aggregateId;
        e.eventType = eventType;
        e.payload = payload;
        e.status = OutboxStatus.PENDING;
        e.createdAt = Instant.now();
        e.retryCount = 0;
        return e;
    }

    public void markProcessed() {
        status = OutboxStatus.PROCESSED;
        processedAt = Instant.now();
        lastError = null;
    }

    /** Başarısız denemeyi kaydeder; eşik aşılırsa FAILED'a çeker ki kuyruğu kilitlemesin. */
    public void registerFailure(String error, int maxRetries) {
        retryCount++;
        lastError = error == null ? null : error.substring(0, Math.min(error.length(), 1000));
        if (retryCount >= maxRetries) {
            status = OutboxStatus.FAILED;
        }
    }

    /** FAILED bir kaydı elle yeniden kuyruğa almak için. */
    public void requeue() {
        status = OutboxStatus.PENDING;
        retryCount = 0;
        lastError = null;
    }

    public String getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getLastError() {
        return lastError;
    }
}
