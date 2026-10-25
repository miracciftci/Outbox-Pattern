package com.example.outbox.config;

import java.util.List;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * SADECE DEMO İÇİN: Kafka kurmadan projeyi çalıştırabilmek adına uygulama
 * içinde tek node'luk bir Kafka (KRaft) broker başlatır.
 * Gerçek Kafka kullanmak için: demo.embedded-kafka.enabled=false (docker profili).
 */
@Configuration
@ConditionalOnProperty(name = "demo.embedded-kafka.enabled", havingValue = "true")
public class EmbeddedKafkaConfig {

    @Bean
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        return new EmbeddedKafkaKraftBroker(1, 3);
    }

    /**
     * KRaft gömülü broker rastgele bir portta açılır; producer/consumer/admin'in
     * bu adrese bağlanması için KafkaProperties'teki bootstrap-servers'ı günceller.
     */
    @Bean
    public static BeanPostProcessor embeddedKafkaBootstrapServers(ObjectProvider<EmbeddedKafkaBroker> broker) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) {
                if (bean instanceof KafkaProperties properties) {
                    properties.setBootstrapServers(List.of(broker.getObject().getBrokersAsString()));
                }
                return bean;
            }
        };
    }
}
