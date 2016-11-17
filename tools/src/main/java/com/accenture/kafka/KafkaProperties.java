package com.accenture.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by THINK on 2016/11/17.
 */
@ConfigurationProperties("kafka")
@Data
public class KafkaProperties {
    String zookeeperConnectionString;
    String brokersAddress;

    @Data
    @ConfigurationProperties("kafka.embedded")
    public static class Embedded {
        private boolean enabled = true;
        private String logFilenamePattern = "/tmp/kafka-logs";
        private int port;
        private int brokerCount = 1;
        private int partitions = 1;
    }

    @Data
    @ConfigurationProperties("kafka.consumer")
    public static class Consumer {
        private String group;
        private boolean batch = true;
    }

    @Data
    @ConfigurationProperties("kafka.producer")
    public static class Producer {
        //String bootstrap_servers;
        Integer metadata_max_age_ms;
        Integer send_buffer_bytes;
        Integer receive_buffer_bytes;
        String client_id;
        Integer reconnect_backoff_ms;
        Integer retry_backoff_ms;
        Integer metrics_sample_window_ms;
        String metrics_num_samples;
        String metric_reporters;
        String security_protocol;
        Integer connections_max_idle_ms;
        Integer request_timeout_ms;
        Integer metadata_fetch_timeout_ms;
        Integer batch_size;
        String acks;
        Integer timeout_ms;
        Integer linger_ms;
        Integer max_request_size;
        Integer max_block_ms;
        String block_on_buffer_full;
        String buffer_memory;
        String compression_type;
        String retries;
        String key_serializer;
        String value_serializer;
        String partitioner_class;
        String interceptor_classes;

        public Map<String, Object> getProducerConfig() {
            Map<String, Object> map = new HashMap<>();
            //map.put("bootstrap.servers", bootstrap_servers);
            map.put("metadata.max.age.ms", metadata_max_age_ms);
            map.put("send.buffer.bytes", send_buffer_bytes);
            map.put("receive.buffer.bytes", receive_buffer_bytes);
            map.put("client.id", client_id);
            map.put("reconnect.backoff.ms", reconnect_backoff_ms);
            map.put("retry.backoff.ms", retry_backoff_ms);
            map.put("metrics.sample.window.ms", metrics_sample_window_ms);
            map.put("metrics.num.samples", metrics_num_samples);
            map.put("metric.reporters", metric_reporters);
            map.put("security.protocol", security_protocol);
            map.put("connections.max.idle.ms", connections_max_idle_ms);
            map.put("request.timeout.ms", request_timeout_ms);
            map.put("metadata.fetch.timeout.ms", metadata_fetch_timeout_ms);
            map.put("batch.size", batch_size);
            map.put("acks", acks);
            map.put("timeout.ms", timeout_ms);
            map.put("linger.ms", linger_ms);
            map.put("max.request.size", max_request_size);
            map.put("max.block.ms", max_block_ms);
            map.put("block.on.buffer.full", block_on_buffer_full);
            map.put("buffer.memory", buffer_memory);
            map.put("compression.type", compression_type);
            map.put("retries", retries);
            map.put("key.serializer", key_serializer);
            map.put("value.serializer", value_serializer);
            map.put("partitioner.class", partitioner_class);
            map.put("interceptor.classes", interceptor_classes);
            return map;
        }
    }
}