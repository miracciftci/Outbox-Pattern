package com.example.outbox.order;

import java.util.Map;

import com.example.outbox.demo.SimulatedFailureException;
import com.example.outbox.messaging.KafkaEventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * YANLIŞ YOL (karşılaştırma için): Dual Write.
 *
 * @Transactional yalnızca veritabanı işlemini kapsar; Kafka bu transaction'ın
 * parçası değildir. DB ile broker arasında atomiklik yoktur:
 *
 *  - HAYALET EVENT : publish başarılı, sonra DB rollback  -> tüketici olmayan bir sipariş için iş yapar
 *  - KAYIP EVENT   : DB commit, sonra publish başarısız   -> tüketici hiç haberdar olmaz
 *  - KULLANILABİLİRLİK: broker çökükse sipariş de alınamaz
 */
@Service
public class NaiveOrderService {

    private static final Logger log = LoggerFactory.getLogger(NaiveOrderService.class);

    private final OrderRepository orderRepository;
    private final KafkaEventPublisher publisher;
    private final ObjectMapper objectMapper;
    private final String topic;

    public NaiveOrderService(OrderRepository orderRepository, KafkaEventPublisher publisher,
                             ObjectMapper objectMapper, @Value("${outbox.topic}") String topic) {
        this.orderRepository = orderRepository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request, boolean failAfterPublish) {
        Order order = Order.create(request.customerId(), "TRY");
        request.items().forEach(i -> order.addItem(i.productId(), i.quantity(), i.unitPrice()));

        orderRepository.save(order);                                                   // DB

        OrderCreatedEvent event = OrderCreatedEvent.from(order);
        publisher.publish(topic, order.getId(), toJson(event),                         // Broker
                Map.of("eventId", event.eventId(), "eventType", event.eventType()));
        log.info("[NAIVE] Sipariş {} için event doğrudan Kafka'ya gönderildi", order.getId());

        if (failAfterPublish) {
            // Örn. commit sırasında constraint ihlali, deadlock, timeout...
            log.error("[NAIVE] Publish sonrası hata -> DB ROLLBACK. Ama event Kafka'ya çoktan gitti: HAYALET EVENT! orderId={}",
                    order.getId());
            throw new SimulatedFailureException(
                    "Sipariş geri alındı ama OrderCreated event'i Kafka'ya gitmişti (hayalet event). orderId=" + order.getId());
        }
        return OrderResponse.from(order);
    }   // commit burada gerçekleşir

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
