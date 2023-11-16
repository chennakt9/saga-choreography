package com.chenna.orderms.service;

import com.chenna.orderms.dto.OrderEvent;
import com.chenna.orderms.entity.OrderTable;
import com.chenna.orderms.entity.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class ReverseOrder {

    @Autowired
    private OrderRepository orderRepository;

    @KafkaListener(topics = "reversed-orders", groupId = "orders-group")
    public void reverseOrder(String event) {
        System.out.println("Reverse order event: " + event);

        try {
            OrderEvent orderEvent = new ObjectMapper().readValue(event, OrderEvent.class);

            Optional<OrderTable> order = orderRepository.findById(orderEvent.getOrder().getOrderId());

            order.ifPresent(o -> {
                o.setStatus("Failed");
                orderRepository.save(o);
            });
        } catch (Exception e) {
            System.out.println("Exception occurred while reverting order details");
        }
    }
}
