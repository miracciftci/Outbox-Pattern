package com.example.outbox.outbox;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/outbox")
public class OutboxController {

    private final OutboxRepository outboxRepository;
    private final OutboxMetrics metrics;

    public OutboxController(OutboxRepository outboxRepository, OutboxMetrics metrics) {
        this.outboxRepository = outboxRepository;
        this.metrics = metrics;
    }

    @GetMapping
    public List<OutboxEvent> list() {
        return outboxRepository.findTop50ByOrderByCreatedAtDesc();
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return Map.of(
                "pending", metrics.pendingCount(),
                "failed", metrics.failedCount(),
                "oldestPendingAgeSeconds", metrics.oldestPendingAgeSeconds());
    }

    /** FAILED durumundaki bir event'i (sorun giderildikten sonra) yeniden kuyruğa alır. */
    @PostMapping("/{id}/retry")
    @Transactional
    public OutboxEvent retry(@PathVariable String id) {
        OutboxEvent event = outboxRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        event.requeue();
        return event;
    }
}
