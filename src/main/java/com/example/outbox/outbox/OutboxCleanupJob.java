package com.example.outbox.outbox;

import java.time.Duration;
import java.time.Instant;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox tablosu sürekli büyür. İşlenmiş kayıtları belirli bir süre sonra siler.
 * (Yüksek hacimde tarih bazlı partition kullanıp eski partition'ları drop etmek daha verimlidir.)
 */
@Component
public class OutboxCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupJob.class);

    private final OutboxRepository outboxRepository;
    private final Duration retention;

    public OutboxCleanupJob(OutboxRepository outboxRepository,
                            @Value("${outbox.cleanup.retention}") Duration retention) {
        this.outboxRepository = outboxRepository;
        this.retention = retention;
    }

    @Scheduled(fixedDelayString = "${outbox.cleanup.fixed-delay}")
    @SchedulerLock(name = "outboxCleanup")
    @Transactional
    public void cleanup() {
        int deleted = outboxRepository.deleteProcessedBefore(Instant.now().minus(retention));
        if (deleted > 0) {
            log.info("Outbox temizliği: {} işlenmiş kayıt silindi", deleted);
        }
    }
}
