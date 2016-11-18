package com.accenture.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */
public class ListenerCfbAdapater<K, V> implements ListenerContainerFactoryBuilder<K, V> {
    protected final KafkaDetail kafkaDetail;
    protected final Map<String, Object> configs;

    public ListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final Map<String, Object> configs) {
        this.kafkaDetail = kafkaDetail;
        this.configs = configs;
    }


    @Override
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(configs);
        adjustConsumerConfigs(props);
        return props;
    }

    @Override
    public ConsumerFactory<K, V> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Override
    public KafkaListenerContainerFactory containerFactory() {
        ConcurrentKafkaListenerContainerFactory<K, V> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        adjustConcurrentKafkaListenerContainerFactory(factory);
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(Math.max(1, kafkaDetail.maxPartitions));
        ContainerProperties containerProperties = factory.getContainerProperties();
        adjustContainerProperties(containerProperties);
        return factory;
    }

    @Override
    public void adjustConsumerConfigs(final Map<String, Object> consumerConfigs) {
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDetail.kafkaConnection.brokersAddress);
    }

    @Override
    public void adjustContainerProperties(final ContainerProperties containerProperties) {

    }

    @Override
    public void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {

    }
}
