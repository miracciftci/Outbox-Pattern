package com.example.outbox.demo;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

/**
 * Sunumdaki hata senaryolarını canlı göstermek için açılıp kapatılan anahtarlar.
 * Gerçek bir projede bulunmaz.
 */
@Component
public class DemoSwitches {

    /** Relay hiç çalışmasın: event'lerin outbox'ta PENDING olarak biriktiğini göstermek için. */
    private final AtomicBoolean relayPaused = new AtomicBoolean(false);

    /** Broker erişilemez gibi davran: her publish denemesi hata versin. */
    private final AtomicBoolean brokerDown = new AtomicBoolean(false);

    /** Relay bir sonraki turda Kafka'ya gönderip processed_at'i yazamadan "çöksün" (tek seferlik). */
    private final AtomicBoolean crashAfterSendOnce = new AtomicBoolean(false);

    public boolean isRelayPaused() {
        return relayPaused.get();
    }

    public void setRelayPaused(boolean value) {
        relayPaused.set(value);
    }

    public boolean isBrokerDown() {
        return brokerDown.get();
    }

    public void setBrokerDown(boolean value) {
        brokerDown.set(value);
    }

    public boolean isCrashAfterSendArmed() {
        return crashAfterSendOnce.get();
    }

    public void armCrashAfterSend() {
        crashAfterSendOnce.set(true);
    }

    /** Anahtar kuruluysa true döner ve anahtarı sıfırlar. */
    public boolean consumeCrashAfterSend() {
        return crashAfterSendOnce.compareAndSet(true, false);
    }

    public void reset() {
        relayPaused.set(false);
        brokerDown.set(false);
        crashAfterSendOnce.set(false);
    }
}
