package com.example.outbox.order;

import java.util.List;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {

    @EntityGraph(attributePaths = "items")
    List<Order> findTop50ByOrderByCreatedAtDesc();
}
