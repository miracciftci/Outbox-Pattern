package com.example.outbox.order;

import java.math.BigDecimal;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty List<@Valid Item> items) {

    public record Item(
            @NotBlank String productId,
            @Positive int quantity,
            @NotNull @Positive BigDecimal unitPrice) {
    }
}
