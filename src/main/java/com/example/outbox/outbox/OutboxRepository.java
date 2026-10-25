package com.example.outbox.outbox;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Bekleyen kayıtları oluşturulma sırasıyla, satır kilidi alarak okur.
     * SKIP LOCKED: başka bir relay instance'ının kilitlediği satırları atlar,
     * böylece iki instance aynı event'i aynı anda göndermez.
     * (H2, PostgreSQL, Oracle ve MySQL 8 bu sözdizimini destekler.)
     */
    @Query(value = """
            SELECT * FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at
            FETCH FIRST :limit ROWS ONLY
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingForUpdateSkipLocked(@Param("limit") int limit);

    long countByStatus(OutboxStatus status);

    Optional<OutboxEvent> findFirstByStatusOrderByCreatedAtAsc(OutboxStatus status);

    List<OutboxEvent> findTop50ByOrderByCreatedAtDesc();

    @Modifying
    @Query("DELETE FROM OutboxEvent e WHERE e.status = com.example.outbox.outbox.OutboxStatus.PROCESSED AND e.processedAt < :before")
    int deleteProcessedBefore(@Param("before") Instant before);
}
