package com.example.outbox.outbox;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.example.outbox.demo.DemoSwitches;
import com.example.outbox.demo.SimulatedFailureException;
import com.example.outbox.messaging.KafkaEventPublisher;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * MESSAGE RELAY - Polling Publisher.
 *
 * Periyodik olarak outbox'taki bekleyen kayıtları okur, Kafka'ya gönderir,
 * ack alınca PROCESSED olarak işaretler.
 *
 * Teslimat garantisi AT-LEAST-ONCE'tır: Kafka'ya gönderip processed_at'i
 * yazamadan çökerse, aynı event bir sonraki turda tekrar gönderilir.
 * Duplicate'leri tüketici taraf (inbox) yakalar.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final KafkaEventPublisher publisher;
    private final TransactionTemplate transactionTemplate;
    private final DemoSwitches switches;
    private final int batchSize;
    private final int maxRetries;

    public OutboxRelay(OutboxRepository outboxRepository,
                       KafkaEventPublisher publisher,
                       TransactionTemplate transactionTemplate,
                       DemoSwitches switches,
                       @Value("${outbox.relay.batch-size}") int batchSize,
                       @Value("${outbox.relay.max-retries}") int maxRetries) {
        this.outboxRepository = outboxRepository;
        this.publisher = publisher;
        this.transactionTemplate = transactionTemplate;
        this.switches = switches;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay}")
    @SchedulerLock(name = "outboxRelay", lockAtMostFor = "PT30S")
    public void relay() {
        if (switches.isRelayPaused()) {
            return;
        }
        try {
            transactionTemplate.executeWithoutResult(status -> publishPendingBatch());
        } catch (SimulatedFailureException e) {
            log.error("RELAY ÇÖKTÜ: {} -> transaction rollback, processed_at yazılmadı. "
                    + "Event'ler bir sonraki turda TEKRAR gönderilecek (duplicate).", e.getMessage());
        }
    }

    /** Tek transaction: kilitle -> gönder -> işaretle -> commit (kilitler bırakılır). */
    private void publishPendingBatch() {
        List<OutboxEvent> batch = outboxRepository.findPendingForUpdateSkipLocked(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent event : batch) {
            try {
                RecordMetadata meta = publisher.publish(
                        topicOf(event),
                        event.getAggregateId(),                       // key = orderId -> aynı partition, sıra korunur
                        event.getPayload(),
                        Map.of("eventId", event.getId(), "eventType", event.getEventType())
                ).getRecordMetadata();

                log.info("Yayınlandı: {} eventId={} key={} -> {}-{}@{}",
                        event.getEventType(), event.getId(), event.getAggregateId(),
                        meta.topic(), meta.partition(), meta.offset());
            } catch (Exception e) {
                event.registerFailure(e.getMessage(), maxRetries);
                log.warn("Gönderilemedi: eventId={} deneme={}/{} durum={} hata={}",
                        event.getId(), event.getRetryCount(), maxRetries, event.getStatus(), e.getMessage());
                // Sırayı korumak için bu turu burada bitir; sonraki event'ler bir sonraki turda denenir.
                return;
            }

            if (switches.consumeCrashAfterSend()) {
                // Kafka ack verdi ama processed_at yazılmadan süreç "çöktü"
                throw new SimulatedFailureException("eventId=" + event.getId() + " Kafka'ya gönderildi, ardından relay çöktü");
            }

            event.markProcessed();
        }
    }

    /** aggregate_type'a göre topic seçimi: "Order" -> "order-events". */
    private String topicOf(OutboxEvent event) {
        return event.getAggregateType().toLowerCase(Locale.ROOT) + "-events";
    }
}
