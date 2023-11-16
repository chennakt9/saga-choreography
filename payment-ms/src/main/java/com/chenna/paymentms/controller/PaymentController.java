package com.chenna.paymentms.controller;

import com.chenna.paymentms.dto.CustomerOrder;
import com.chenna.paymentms.dto.OrderEvent;
import com.chenna.paymentms.dto.PaymentEvent;
import com.chenna.paymentms.entity.Payment;
import com.chenna.paymentms.entity.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.criteria.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;


@Controller
public class PaymentController {

    @Autowired
    private PaymentRepository repository;

    @Autowired
    private KafkaTemplate<String, PaymentEvent> paymentEventKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, OrderEvent> orderEventKafkaTemplate;

    @KafkaListener(topics = "new-orders", groupId = "orders-group")
    public void processPayment(String event) throws Exception {
        System.out.println("Process payment event: " + event);

        OrderEvent orderEvent = new ObjectMapper().readValue(event, OrderEvent.class);
        CustomerOrder order = orderEvent.getOrder();

        Payment payment = new Payment();
        payment.setAmount(order.getAmount());
        payment.setMode(order.getPaymentMode());
        payment.setOrderId(order.getOrderId());
        payment.setStatus("Success");

        try {
            repository.save(payment);

            PaymentEvent event1 = new PaymentEvent();
            event1.setOrder(order);
            event1.setType("PAYMENT_CREATED");

            paymentEventKafkaTemplate.send("new-payments", event1);
        } catch (Exception e) {
            payment.setOrderId(order.getOrderId());
            payment.setStatus("Failed");
            repository.save(payment);

            OrderEvent orderEvent1 = new OrderEvent();
            orderEvent1.setOrder(order);
            orderEvent1.setType("ORDER_REVERSED");

            orderEventKafkaTemplate.send("reversed-orders", orderEvent1);

            throw e;
        }
    }
}
