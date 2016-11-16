package com.accenture.kafka;

import org.springframework.cglib.proxy.Factory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.Map;

/**
 * Created by THINK on 2016/11/15.
 */
public interface ListenerContainerFactoryBuilder<K, V> {
    Map<String, Object> consumerConfigs();

    ConsumerFactory<K, V> consumerFactory();

    KafkaListenerContainerFactory containerFactory();

    void adjustConsumerConfigs(Map<String, Object> consumerConfigs);
    void adjustContainerProperties(ContainerProperties containerProperties);
    void adjustConcurrentKafkaListenerContainerFactory( ConcurrentKafkaListenerContainerFactory<K, V> factory);
}
