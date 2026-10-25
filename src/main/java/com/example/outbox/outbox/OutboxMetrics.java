package com.example.outbox.outbox;

import java.time.Duration;
import java.time.Instant;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * İzleme: en önemli iki metrik bekleyen kayıt sayısı ve en eski bekleyen
 * kaydın yaşıdır. Yaş sürekli artıyorsa relay takılmış demektir.
 *
 * /actuator/metrics/outbox.pending.count
 * /actuator/metrics/outbox.oldest.pending.age.seconds
 * /actuator/metrics/outbox.failed.count
 */
@Component
public class OutboxMetrics {

    private final OutboxRepository outboxRepository;

    public OutboxMetrics(OutboxRepository outboxRepository, MeterRegistry registry) {
        this.outboxRepository = outboxRepository;
        Gauge.builder("outbox.pending.count", this::pendingCount).register(registry);
        Gauge.builder("outbox.failed.count", this::failedCount).register(registry);
        Gauge.builder("outbox.oldest.pending.age.seconds", this::oldestPendingAgeSeconds).register(registry);
    }

    public long pendingCount() {
        return outboxRepository.countByStatus(OutboxStatus.PENDING);
    }

    public long failedCount() {
        return outboxRepository.countByStatus(OutboxStatus.FAILED);
    }

    public long oldestPendingAgeSeconds() {
        return outboxRepository.findFirstByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING)
                .map(e -> Duration.between(e.getCreatedAt(), Instant.now()).toSeconds())
                .orElse(0L);
    }
}
