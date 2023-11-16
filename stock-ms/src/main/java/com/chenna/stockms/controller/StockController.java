package com.chenna.stockms.controller;

import com.chenna.stockms.dto.CustomerOrder;
import com.chenna.stockms.dto.DeliveryEvent;
import com.chenna.stockms.dto.PaymentEvent;
import com.chenna.stockms.dto.Stock;
import com.chenna.stockms.entity.StockRepository;
import com.chenna.stockms.entity.WareHouse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class StockController {
    @Autowired
    private StockRepository stockRepository;

    @Autowired
    private KafkaTemplate<String, DeliveryEvent> deliveryEventKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, PaymentEvent> paymentEventKafkaTemplate;

    @KafkaListener(topics = "new-payments", groupId = "payments-group")
    public void updateStock(String paymentEvent) throws Exception{
        System.out.println("Update inventory for order: " + paymentEvent);

        DeliveryEvent event = new DeliveryEvent();

        PaymentEvent p = new ObjectMapper().readValue(paymentEvent, PaymentEvent.class);
        CustomerOrder order = p.getOrder();

        try {
            Iterable<WareHouse> inventories = stockRepository.findByItem(order.getItem());

            boolean exists = inventories.iterator().hasNext();

            if(!exists) {
                System.out.println("Stock not exist so reverting the order");
                throw new Exception("Stock not available");
            }

            inventories.forEach(i -> {
                i.setQuantity(i.getQuantity() - order.getQuantity());

                stockRepository.save(i);
            });

            event.setType("STOCK_UPDATED");
            event.setOrder(p.getOrder());
            deliveryEventKafkaTemplate.send("new-stock", event);
        } catch (Exception e) {
            PaymentEvent paymentEvent1 = new PaymentEvent();
            paymentEvent1.setOrder(order);
            paymentEvent1.setType("PAYMENT_REVERSED");
            paymentEventKafkaTemplate.send("reversed-payments", paymentEvent1);
        }
    }

    @PostMapping("/addItems")
    public void addItems(@RequestBody Stock stock) {
        Iterable<WareHouse> items = stockRepository.findByItem(stock.getItem());

        if (items.iterator().hasNext()) {
            items.forEach(i -> {
                i.setQuantity(stock.getQuantity() + i.getQuantity());
                stockRepository.save(i);
            });
        } else {
            WareHouse i = new WareHouse();
            i.setItem(stock.getItem());
            i.setQuantity(stock.getQuantity());
            stockRepository.save(i);
        }
    }
}
