package com.javatechie.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javatechie.dto.PaymentRequest;
import com.javatechie.dto.PaytmRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

@RestController
public class PaytmController {

    @Value("${paytm.producer.topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/publish/{message}")
    public void sendMessage(@PathVariable String message) {
        for (int i = 0; i <= 10000; i++) {
            kafkaTemplate.send(topicName, message + i);
        }
    }

    @PostMapping("/paytm/payment")
    public String doPayment(@RequestBody PaytmRequest<PaymentRequest> paytmRequest) throws JsonProcessingException {
        for (int i = 1; i <= 500; i++) {

            PaymentRequest paymentRequest = paytmRequest.getPayload();
            paymentRequest.setSrcAc("SRC_AC" + i);
            paymentRequest.setDestAc("SRC_DEST" + i);
            paymentRequest.setAmount(new Random().nextInt(10000));
            paymentRequest.setTransactionId(UUID.randomUUID().toString());
            paymentRequest.setTxDate(new Date());
            kafkaTemplate.send(topicName, paymentRequest);
        }
        // send object as string
        //kafkaTemplate.send(topicName,new ObjectMapper().writeValueAsString(paymentRequest));
        return "payment instantiate successfully...";
    }
}
