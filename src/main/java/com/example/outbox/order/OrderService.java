package com.example.outbox.order;

import java.util.List;

import com.example.outbox.demo.SimulatedFailureException;
import com.example.outbox.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DOĞRU YOL: Transactional Outbox.
 *
 * Sipariş ve event AYNI local transaction'da, AYNI veritabanına yazılır.
 * Ya ikisi de commit edilir ya hiçbiri. Kafka'ya burada hiçbir şey gönderilmez;
 * o iş OutboxRelay'in.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;

    public OrderService(OrderRepository orderRepository, OutboxWriter outboxWriter) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request, boolean simulateFailure) {
        Order order = Order.create(request.customerId(), "TRY");
        request.items().forEach(i -> order.addItem(i.productId(), i.quantity(), i.unitPrice()));

        orderRepository.save(order);                                       // 1) iş verisi

        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        outboxWriter.write("Order", order.getId(), event.eventType(),      // 2) outbox kaydı
                event.eventId(), event);

        if (simulateFailure) {
            // Commit'ten önce bir hata: sipariş de outbox kaydı da birlikte geri alınır.
            // Kafka'ya hiçbir şey gitmediği için "hayalet event" oluşmaz.
            log.warn("Sipariş {} için hata simüle edildi -> sipariş + outbox ROLLBACK", order.getId());
            throw new SimulatedFailureException("Simüle edilmiş hata: sipariş ve outbox kaydı birlikte geri alındı");
        }

        log.info("Sipariş {} oluşturuldu, OrderCreated event'i outbox'a yazıldı (henüz Kafka'ya gitmedi)", order.getId());
        return OrderResponse.from(order);
    }   // <- COMMIT burada: orders + order_items + outbox tek seferde

    @Transactional(readOnly = true)
    public List<OrderResponse> findRecent() {
        return orderRepository.findTop50ByOrderByCreatedAtDesc().stream()
                .map(OrderResponse::from)
                .toList();
    }
}
