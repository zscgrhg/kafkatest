package com.accenture.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

/**
 * Created by THINK on 2016/11/18.
 */
@RestController
public class Hello {

    final Random random = new Random();
    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("hello")
    public String sayHelloToKafka(){

        kafkaTemplate.send("local1",(Integer)"abcde".hashCode(), random.nextInt()+"->");
        return "1";
    }
}
