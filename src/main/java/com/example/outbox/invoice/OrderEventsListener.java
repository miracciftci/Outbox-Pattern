package com.example.outbox.invoice;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * FATURA SERVİSİ - Idempotent consumer.
 *
 * Outbox at-least-once teslimat sağlar; aynı event birden fazla gelebilir.
 * Bu yüzden her event'in id'si inbox tablosunda tutulur ve iş verisiyle
 * (fatura) AYNI local transaction'da yazılır.
 *
 * Offset, metot (ve dolayısıyla DB transaction'ı) başarıyla bittikten sonra
 * commit edilir (ack-mode=record, auto commit kapalı).
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final InboxRepository inboxRepository;
    private final InvoiceRepository invoiceRepository;
    private final ObjectMapper objectMapper;

    public OrderEventsListener(InboxRepository inboxRepository, InvoiceRepository invoiceRepository,
                               ObjectMapper objectMapper) {
        this.inboxRepository = inboxRepository;
        this.invoiceRepository = invoiceRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${outbox.topic}", groupId = "invoice-service")
    @Transactional
    public void on(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String eventId = header(record, "eventId");
        String eventType = header(record, "eventType");

        log.info("Alındı: {} eventId={} key={} partition={} offset={}",
                eventType, eventId, record.key(), record.partition(), record.offset());

        if (inboxRepository.existsById(eventId)) {
            log.warn("DUPLICATE yakalandı, atlanıyor: eventId={} (inbox'ta zaten var)", eventId);
            return;
        }

        if ("OrderCreated".equals(eventType)) {
            OrderCreatedMessage msg = objectMapper.readValue(record.value(), OrderCreatedMessage.class);
            Invoice invoice = invoiceRepository.save(Invoice.forOrder(
                    msg.orderId(), msg.customerId(), msg.totalAmount(), msg.currency(), eventId));
            log.info("Fatura oluşturuldu: invoiceId={} orderId={} tutar={} {}",
                    invoice.getId(), msg.orderId(), msg.totalAmount(), msg.currency());
        }

        // İş verisi + inbox kaydı aynı transaction'da. Eşzamanlı iki duplicate gelirse
        // PK çakışması transaction'ı geri alır, mesaj tekrar denenir ve ikinci seferde atlanır.
        inboxRepository.save(new InboxMessage(eventId, eventType));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Eksik header: " + name);
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
