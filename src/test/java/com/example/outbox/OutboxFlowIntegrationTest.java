package com.example.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.example.outbox.demo.DemoSwitches;
import com.example.outbox.demo.SimulatedFailureException;
import com.example.outbox.invoice.InvoiceRepository;
import com.example.outbox.messaging.BrokerUnavailableException;
import com.example.outbox.order.CreateOrderRequest;
import com.example.outbox.order.NaiveOrderService;
import com.example.outbox.order.OrderRepository;
import com.example.outbox.order.OrderResponse;
import com.example.outbox.order.OrderService;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxRepository;
import com.example.outbox.outbox.OutboxStatus;
import com.example.outbox.outbox.OutboxWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.IllegalTransactionStateException;

@SpringBootTest(properties = {
        "outbox.relay.fixed-delay=200",
        "outbox.relay.max-retries=3"
})
class OutboxFlowIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired OrderService orderService;
    @Autowired NaiveOrderService naiveOrderService;
    @Autowired OrderRepository orderRepository;
    @Autowired OutboxRepository outboxRepository;
    @Autowired OutboxWriter outboxWriter;
    @Autowired InvoiceRepository invoiceRepository;
    @Autowired DemoSwitches switches;

    @AfterEach
    void resetSwitches() {
        switches.reset();
    }

    @Test
    void siparisVeOutboxAyniTransactiondaYazilirVeFaturaOlusur() {
        switches.setRelayPaused(true);
        OrderResponse order = orderService.createOrder(request(), false);

        // Relay durduruldu: event outbox'ta bekliyor, fatura yok
        OutboxEvent event = outboxFor(order.id());
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(invoiceRepository.countByOrderId(order.id())).isZero();

        switches.setRelayPaused(false);
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(outboxFor(order.id()).getStatus()).isEqualTo(OutboxStatus.PROCESSED);
            assertThat(invoiceRepository.countByOrderId(order.id())).isEqualTo(1);
        });
    }

    @Test
    void commitOncesiHatadaSiparisDeOutboxDaGeriAlinir() {
        long ordersBefore = orderRepository.count();
        long outboxBefore = outboxRepository.count();

        assertThatThrownBy(() -> orderService.createOrder(request(), true))
                .isInstanceOf(SimulatedFailureException.class);

        assertThat(orderRepository.count()).isEqualTo(ordersBefore);
        assertThat(outboxRepository.count()).isEqualTo(outboxBefore);
    }

    @Test
    void brokerCokukkenSiparisAlinirEventBekler() {
        switches.setBrokerDown(true);
        OrderResponse order = orderService.createOrder(request(), false);

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(outboxFor(order.id()).getRetryCount()).isGreaterThanOrEqualTo(1));
        assertThat(orderRepository.existsById(order.id())).isTrue();
        assertThat(invoiceRepository.countByOrderId(order.id())).isZero();

        switches.setBrokerDown(false);
        OutboxEvent event = outboxFor(order.id());
        if (event.getStatus() == OutboxStatus.FAILED) {
            event.requeue();
            outboxRepository.save(event);
        }
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(invoiceRepository.countByOrderId(order.id())).isEqualTo(1));
    }

    @Test
    void relayGonderimSonrasiCokerseDuplicateInboxIleYakalanir() {
        switches.setRelayPaused(true);
        OrderResponse order = orderService.createOrder(request(), false);
        switches.armCrashAfterSend();
        switches.setRelayPaused(false);

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(outboxFor(order.id()).getStatus()).isEqualTo(OutboxStatus.PROCESSED));
        // Event Kafka'ya iki kez gitti; ikinci kopyanın da tüketilmesi için biraz bekle
        await().pollDelay(Duration.ofSeconds(2)).atMost(TIMEOUT).untilAsserted(() ->
                assertThat(invoiceRepository.countByOrderId(order.id())).isEqualTo(1));
    }

    @Test
    void naiveYontemdeHayaletEventOlusur() {
        assertThatThrownBy(() -> naiveOrderService.createOrder(request(), true))
                .isInstanceOf(SimulatedFailureException.class)
                .hasMessageContaining("orderId=");

        // Sipariş DB'de yok ama fatura servisi bu sipariş için fatura kesti
        await().atMost(TIMEOUT).until(() -> invoiceRepository.findAll().stream()
                .anyMatch(inv -> !orderRepository.existsById(inv.getOrderId())));
    }

    @Test
    void naiveYontemdeBrokerCokukseSiparisAlinamaz() {
        switches.setBrokerDown(true);
        assertThatThrownBy(() -> naiveOrderService.createOrder(request(), false))
                .isInstanceOf(BrokerUnavailableException.class);
    }

    @Test
    void outboxWriterTransactionDisindaCagrilamaz() {
        assertThatThrownBy(() -> outboxWriter.write("Order", "x", "OrderCreated", UUID.randomUUID().toString(), "{}"))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    private OutboxEvent outboxFor(String orderId) {
        return outboxRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(orderId))
                .findFirst()
                .orElseThrow();
    }

    private static CreateOrderRequest request() {
        return new CreateOrderRequest(
                "C-" + UUID.randomUUID().toString().substring(0, 5),
                List.of(new CreateOrderRequest.Item("P-100", 2, new BigDecimal("149.90")),
                        new CreateOrderRequest.Item("P-200", 1, new BigDecimal("899.00"))));
    }
}
