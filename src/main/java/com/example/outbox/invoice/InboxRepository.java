package com.example.outbox.invoice;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxRepository extends JpaRepository<InboxMessage, String> {

    List<InboxMessage> findTop50ByOrderByProcessedAtDesc();
}
