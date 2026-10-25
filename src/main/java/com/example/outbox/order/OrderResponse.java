package com.example.outbox.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
        String id,
        String customerId,
        BigDecimal totalAmount,
        String currency,
        OrderStatus status,
        Instant createdAt,
        List<Line> items) {

    public record Line(String productId, int quantity, BigDecimal unitPrice) {
    }

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getItems().stream()
                        .map(i -> new Line(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                        .toList());
    }
}
