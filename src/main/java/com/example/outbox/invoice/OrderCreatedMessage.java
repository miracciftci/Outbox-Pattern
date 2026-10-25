package com.example.outbox.invoice;

import java.math.BigDecimal;

/**
 * Fatura servisinin, OrderCreated event'inden ihtiyaç duyduğu alanlar.
 * Tüketici, üreticinin sınıflarına bağımlı değildir; kendi sözleşmesini tanımlar.
 * (Bilinmeyen alanlar Spring Boot'un ObjectMapper'ı tarafından yok sayılır.)
 */
public record OrderCreatedMessage(
        String eventId,
        int schemaVersion,
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        String currency) {
}
