package com.example.outbox;

import java.util.Locale;

import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT30S")
public class OrderOutboxApplication {

    public static void main(String[] args) {
        // Türkçe locale'de Kafka "classic".toUpperCase() -> "CLASSİC" yapıp çöküyor (meşhur Türkçe I sorunu)
        Locale.setDefault(Locale.ENGLISH);
        SpringApplication.run(OrderOutboxApplication.class, args);
    }
}
