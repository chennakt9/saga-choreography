package com.chenna.paymentms.service;

import com.chenna.paymentms.dto.CustomerOrder;
import com.chenna.paymentms.dto.OrderEvent;
import com.chenna.paymentms.dto.PaymentEvent;
import com.chenna.paymentms.entity.Payment;
import com.chenna.paymentms.entity.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ReversePayment {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = "reversed-payments", groupId = "payments-group")
    public void reversePayment(String event) {
        System.out.println("Reverse payment for order" + event);

        try {
            PaymentEvent paymentEvent = new ObjectMapper().readValue(event, PaymentEvent.class);
            CustomerOrder order = paymentEvent.getOrder();

            Iterable<Payment> payments = paymentRepository.findByOrderId(order.getOrderId());

            payments.forEach(p -> {
                p.setStatus("Failed");
                paymentRepository.save(p);
            });

            OrderEvent orderEvent = new OrderEvent();
            orderEvent.setOrder(paymentEvent.getOrder());
            orderEvent.setType("ORDER_REVERSED");
            kafkaTemplate.send("reversed-orders", orderEvent);
        } catch (Exception e) {
            System.out.println("Error occurred while reversing payment");
        }
    }

}
