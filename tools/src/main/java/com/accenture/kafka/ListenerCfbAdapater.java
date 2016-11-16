package com.accenture.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.core.serializer.Deserializer;
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
    protected final String groupId;
    protected final Class<? extends Deserializer<K>> deserializerK;
    protected final Class<? extends Deserializer<V>> deserializerV;
    public static final long defaultPollTimeout = 3000;
    protected final long pollTimeout;

    public ListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV,
            long pollTimeout) {
        this.kafkaDetail = kafkaDetail;
        this.groupId = groupId;
        this.deserializerK = deserializerK;
        this.deserializerV = deserializerV;
        this.pollTimeout = pollTimeout;
    }

    public ListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV) {
        this(kafkaDetail, groupId, deserializerK, deserializerV, defaultPollTimeout);
    }


    @Override
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        adjustConsumerConfigs(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDetail.kafkaConnection.brokersAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerK);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerV);
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

    }

    @Override
    public void adjustContainerProperties(final ContainerProperties containerProperties) {
        containerProperties.setPollTimeout(pollTimeout);
    }

    @Override
    public void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {

    }
}
