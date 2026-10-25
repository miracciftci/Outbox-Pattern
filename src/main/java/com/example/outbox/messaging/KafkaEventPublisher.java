package com.example.outbox.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.example.outbox.demo.DemoSwitches;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Kafka'ya senkron (ack bekleyerek) mesaj gönderen ince bir sarmalayıcı.
 * Hem Outbox Relay hem de "naive" (yanlış) örnek bunu kullanır.
 */
@Component
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DemoSwitches switches;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, DemoSwitches switches) {
        this.kafkaTemplate = kafkaTemplate;
        this.switches = switches;
    }

    public SendResult<String, String> publish(String topic, String key, String payload, Map<String, String> headers) {
        if (switches.isBrokerDown()) {
            throw new BrokerUnavailableException("Broker erişilemez (simülasyon)");
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        headers.forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));

        try {
            // Ack gelene kadar bekle: "gönderildi" diyebilmek için broker'ın onayı şart
            return kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerUnavailableException("Gönderim kesildi", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new BrokerUnavailableException("Kafka'ya gönderilemedi: " + e.getMessage(), e);
        }
    }
}
