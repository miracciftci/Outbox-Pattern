package com.example.outbox.invoice;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class InvoiceController {

    private final InvoiceRepository invoiceRepository;
    private final InboxRepository inboxRepository;

    public InvoiceController(InvoiceRepository invoiceRepository, InboxRepository inboxRepository) {
        this.invoiceRepository = invoiceRepository;
        this.inboxRepository = inboxRepository;
    }

    @GetMapping("/invoices")
    public List<Invoice> invoices() {
        return invoiceRepository.findTop50ByOrderByCreatedAtDesc();
    }

    @GetMapping("/inbox")
    public List<InboxMessage> inbox() {
        return inboxRepository.findTop50ByOrderByProcessedAtDesc();
    }
}
