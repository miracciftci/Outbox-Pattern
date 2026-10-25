package com.example.outbox.invoice;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "invoices")
public class Invoice {

    @Id
    private String id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Invoice() {
    }

    public static Invoice forOrder(String orderId, String customerId, BigDecimal amount,
                                   String currency, String eventId) {
        Invoice invoice = new Invoice();
        invoice.id = UUID.randomUUID().toString();
        invoice.orderId = orderId;
        invoice.customerId = customerId;
        invoice.amount = amount;
        invoice.currency = currency;
        invoice.eventId = eventId;
        invoice.createdAt = Instant.now();
        return invoice;
    }

    public String getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getEventId() {
        return eventId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
