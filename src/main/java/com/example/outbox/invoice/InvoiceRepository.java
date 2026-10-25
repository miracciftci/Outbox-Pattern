package com.example.outbox.invoice;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface InvoiceRepository extends JpaRepository<Invoice, String> {

    List<Invoice> findTop50ByOrderByCreatedAtDesc();

    long countByOrderId(String orderId);
}
