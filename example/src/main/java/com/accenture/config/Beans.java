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

import static com.accenture.config.Profiles.DEV;
import static com.accenture.config.Profiles.TEST;
import static com.accenture.kafka.KafkaAutoConfiguration.KafkaListenerContainerFactoryAutoConfiguration.KAFKA_BATCH_LISTERNER_CONTAINER_FACTORY_NAME;

/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
@EnableKafka
public class Beans {


    @Bean
    public Listenner listenner() {
        return new Listenner();
    }

    public static class Listenner {
        @KafkaListener(topics = "topicx", containerFactory = KAFKA_BATCH_LISTERNER_CONTAINER_FACTORY_NAME)
        public void listen(List<ConsumerRecord<String, String>> list) throws InterruptedException {

        }

    }
}
