package com.example.outbox.demo;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final DemoSwitches switches;

    public DemoController(DemoSwitches switches) {
        this.switches = switches;
    }

    @GetMapping("/state")
    public Map<String, Boolean> state() {
        return Map.of(
                "relayPaused", switches.isRelayPaused(),
                "brokerDown", switches.isBrokerDown(),
                "crashAfterSendArmed", switches.isCrashAfterSendArmed());
    }

    @PostMapping("/relay/pause")
    public Map<String, Boolean> pauseRelay() {
        switches.setRelayPaused(true);
        return state();
    }

    @PostMapping("/relay/resume")
    public Map<String, Boolean> resumeRelay() {
        switches.setRelayPaused(false);
        return state();
    }

    @PostMapping("/relay/crash-next")
    public Map<String, Boolean> crashRelayAfterNextSend() {
        switches.armCrashAfterSend();
        return state();
    }

    @PostMapping("/broker/down")
    public Map<String, Boolean> brokerDown() {
        switches.setBrokerDown(true);
        return state();
    }

    @PostMapping("/broker/up")
    public Map<String, Boolean> brokerUp() {
        switches.setBrokerDown(false);
        return state();
    }

    @PostMapping("/reset")
    public Map<String, Boolean> reset() {
        switches.reset();
        return state();
    }
}
