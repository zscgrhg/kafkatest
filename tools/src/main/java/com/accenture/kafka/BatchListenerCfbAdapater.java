package com.accenture.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.core.serializer.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */
public class BatchListenerCfbAdapater<K, V> extends ListenerCfbAdapater<K, V> {


    public BatchListenerCfbAdapater(final KafkaDetail kafkaDetail,final Map<String, Object> configs) {
        super(kafkaDetail, configs);
    }

    @Override
    public void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {
        factory.setBatchListener(true);
    }
}
