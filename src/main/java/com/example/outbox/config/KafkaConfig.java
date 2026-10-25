package com.example.outbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /**
     * 3 partition: aynı siparişin (aynı key) event'leri hep aynı partition'a düşer,
     * farklı siparişler paralel işlenebilir.
     */
    @Bean
    public NewTopic orderEventsTopic(@Value("${outbox.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    /**
     * Consumer tarafında hata yönetimi: 2 kez tekrar dener, yine olmazsa
     * "zehirli" mesajı order-events.DLT topic'ine taşır; kuyruk kilitlenmez.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate),
                new FixedBackOff(1000L, 2));
    }
}
