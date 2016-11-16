package com.accenture.config;

import com.accenture.kafka.BatchListenerCfbAdapater;
import com.accenture.kafka.KafkaConnection;
import com.accenture.kafka.KafkaDetail;
import com.accenture.kafka.ListenerCfbAdapater;
import com.accenture.utils.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.List;

/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
@EnableKafka
@Profile({"dev","test"})
@Slf4j
public class Beans4Dev {
    @Bean
    public KafkaConnection kafka() throws Exception {
        KafkaEmbedded kafka = new KafkaEmbedded(9092, 3, true, 3);
        kafka.start();
        String brokersAsString = kafka.getBrokersAsString();
        log.debug(brokersAsString);
        KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(kafka.getBrokersAsString())
                .isEmbedded(true)
                .zookeeperConnectionString(kafka.getZookeeperConnectionString()).build();
        return kafkaConnection;
    }

    @Bean
    @Primary
    KafkaUtil devKafkaUtil() {
        return new KafkaUtil(false);
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
    kafkaListenerContainerFactory(KafkaDetail kafkaDetail) throws Exception {
        ListenerCfbAdapater<String, String> test =
                new ListenerCfbAdapater(kafkaDetail, "test", StringDeserializer.class, StringDeserializer.class);
        return test.containerFactory();
    }


    @Bean
    public KafkaListenerContainerFactory<?> batchFactory(KafkaDetail kafkaDetail) throws Exception {
        BatchListenerCfbAdapater<String, String> test = new BatchListenerCfbAdapater(kafkaDetail, "test", StringDeserializer.class, StringDeserializer.class);
        return test.containerFactory();
    }

    @Bean
    public Listenner listenner() {
        return new Listenner();
    }

    public static class Listenner {
        @KafkaListener(topics = "topicx", containerFactory = "batchFactory")
        public void listen(List<ConsumerRecord<String, String>> list) throws InterruptedException {
            log.info("----------");
            System.out.println(list.size());
        }

    }
}
