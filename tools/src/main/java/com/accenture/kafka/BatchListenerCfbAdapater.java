package com.accenture.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.core.serializer.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */
public class BatchListenerCfbAdapater<K, V> extends ListenerCfbAdapater<K, V> {
    public static final int DEFAULT_FETCH_MIN_BYTES_CONFIG = 1024 * 256;
    protected final int fetchMinBytes;

    public BatchListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV,
            final long pollTimeout, final int fetch_min_bytes_config) {
        super(kafkaDetail, groupId, deserializerK, deserializerV, pollTimeout);
        fetchMinBytes = fetch_min_bytes_config;
    }

    public BatchListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV, final int fetch_min_bytes_config) {
        super(kafkaDetail, groupId, deserializerK, deserializerV);
        fetchMinBytes = fetch_min_bytes_config;
    }

    public BatchListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV,
            final long pollTimeout) {
        super(kafkaDetail, groupId, deserializerK, deserializerV, pollTimeout);
        this.fetchMinBytes = DEFAULT_FETCH_MIN_BYTES_CONFIG;
    }

    public BatchListenerCfbAdapater(
            final KafkaDetail kafkaDetail,
            final String groupId,
            final Class<? extends Deserializer<K>> deserializerK,
            final Class<? extends Deserializer<V>> deserializerV) {
        super(kafkaDetail, groupId, deserializerK, deserializerV);
        this.fetchMinBytes = DEFAULT_FETCH_MIN_BYTES_CONFIG;
    }


    @Override
    public void adjustConsumerConfigs(final Map<String, Object> consumerConfigs) {
        super.adjustConsumerConfigs(consumerConfigs);
        consumerConfigs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
    }

    @Override
    public void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {
        factory.setBatchListener(true);
    }
}
