package com.example.outbox.demo;

/** Demo amaçlı bilinçli olarak fırlatılan hata. */
public class SimulatedFailureException extends RuntimeException {

    public SimulatedFailureException(String message) {
        super(message);
    }
}
