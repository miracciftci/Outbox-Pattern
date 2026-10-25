package com.example.outbox.order;

import java.util.List;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final NaiveOrderService naiveOrderService;

    public OrderController(OrderService orderService, NaiveOrderService naiveOrderService) {
        this.orderService = orderService;
        this.naiveOrderService = naiveOrderService;
    }

    /** Outbox ile sipariş oluşturma. simulateFailure=true -> commit öncesi hata (her şey rollback). */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse create(@Valid @RequestBody CreateOrderRequest request,
                                @RequestParam(defaultValue = "false") boolean simulateFailure) {
        return orderService.createOrder(request, simulateFailure);
    }

    /** Dual write (yanlış yol). failAfterPublish=true -> hayalet event. */
    @PostMapping("/naive")
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createNaive(@Valid @RequestBody CreateOrderRequest request,
                                     @RequestParam(defaultValue = "false") boolean failAfterPublish) {
        return naiveOrderService.createOrder(request, failAfterPublish);
    }

    @GetMapping
    public List<OrderResponse> list() {
        return orderService.findRecent();
    }
}
