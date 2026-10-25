package com.example.outbox.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Event'i outbox tablosuna yazar.
 *
 * Propagation.MANDATORY: bu metot mutlaka çağıranın transaction'ı içinde
 * çalışmalıdır. Transaction yoksa hata fırlatır; böylece event'in iş
 * verisinden ayrı bir transaction'da yazılması (yani dual write'a geri
 * dönülmesi) yanlışlıkla bile mümkün olmaz.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(String aggregateType, String aggregateId, String eventType, String eventId, Object event) {
        outboxRepository.save(OutboxEvent.create(eventId, aggregateType, aggregateId, eventType, toJson(event)));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Event JSON'a çevrilemedi", e);
        }
    }
}
