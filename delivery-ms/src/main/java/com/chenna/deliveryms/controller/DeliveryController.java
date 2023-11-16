package com.chenna.deliveryms.controller;

import com.chenna.deliveryms.dto.CustomerOrder;
import com.chenna.deliveryms.dto.DeliveryEvent;
import com.chenna.deliveryms.entity.Delivery;
import com.chenna.deliveryms.entity.DeliveryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;

@Controller
public class DeliveryController {

    @Autowired
    private DeliveryRepository repository;

    @Autowired
    private KafkaTemplate<String, DeliveryEvent> kafkaTemplate;

    @KafkaListener(topics = "new-stock", groupId = "stock-group")
    public void deliverOrder(String event) throws Exception {
        System.out.println("Order " + event);

        Delivery shipment = new Delivery();
        DeliveryEvent inventoryEvent = new ObjectMapper().readValue(event, DeliveryEvent.class);
        CustomerOrder order = inventoryEvent.getOrder();

        try {
            if(order.getAddress() == null) {
                throw new Exception("Address not present");
            }

            shipment.setAddress(order.getAddress());
            shipment.setOrderId(order.getOrderId());

            shipment.setStatus("Success");
            repository.save(shipment);
        } catch (Exception e) {
            shipment.setOrderId(order.getOrderId());
            shipment.setStatus("Failed");
            repository.save(shipment);

            DeliveryEvent reverseEvent = new DeliveryEvent();
            reverseEvent.setType("STOCK_REVERSED");
            reverseEvent.setOrder(order);
            kafkaTemplate.send("reversed-stock", reverseEvent);
        }

    }
}
