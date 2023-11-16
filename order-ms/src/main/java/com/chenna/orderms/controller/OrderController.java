package com.chenna.orderms.controller;

import com.chenna.orderms.dto.CustomerOrder;
import com.chenna.orderms.dto.OrderEvent;
import com.chenna.orderms.entity.OrderTable;
import com.chenna.orderms.entity.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class OrderController {

    @Autowired
    private OrderRepository repository;

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @PostMapping("/orders")
    public void createOrder(@RequestBody CustomerOrder customerOrder) {
        OrderTable order = new OrderTable();
        order.setAmount(customerOrder.getAmount());
        order.setItem(customerOrder.getItem());
        order.setQuantity(customerOrder.getQuantity());
        order.setStatus("Created");

        try {
            order = repository.save(order);

            customerOrder.setOrderId(order.getId());

            OrderEvent orderEvent = new OrderEvent();
            orderEvent.setOrder(customerOrder);
            orderEvent.setType("ORDER_CREATED");

            kafkaTemplate.send("new-orders", orderEvent);
        } catch (Exception e) {
            order.setStatus("Failed");
            repository.save(order);
            throw e;
        }
    }
}
