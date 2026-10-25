package com.example.outbox.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Dışarıya yayınlanan domain event'i.
 *
 * - Entity'nin kendisi değil, anlamlı bir event: iç şema dışarı sızmaz.
 * - Snapshot: yazma anındaki hali saklanır; relay entity'yi yeniden okumaz.
 * - schemaVersion: şema evrimi için.
 * - eventId: outbox.id ile aynıdır, consumer tarafında deduplication anahtarıdır.
 */
public record OrderCreatedEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        String currency,
        List<Line> items) {

    public static final String TYPE = "OrderCreated";

    public record Line(String productId, int quantity, BigDecimal unitPrice) {
    }

    public static OrderCreatedEvent from(Order order) {
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                TYPE,
                1,
                Instant.now(),
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency(),
                order.getItems().stream()
                        .map(i -> new Line(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                        .toList());
    }
}
