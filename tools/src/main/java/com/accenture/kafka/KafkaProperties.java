package com.accenture.kafka;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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

    /**
     * <a href="http://kafka.apache.org/documentation#newconsumerconfigs">Consumer configs</a>
     */
    @Data
    @ConfigurationProperties("kafka.consumer")
    public static class Consumer {
        String bootstrap_servers;
        String key_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        String value_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        Integer fetch_min_bytes;
        String group_id;
        Integer heartbeat_interval_ms;
        Integer max_partition_fetch_bytes;
        Integer session_timeout_ms;
        String ssl_key_password;
        String ssl_keystore_location;
        String ssl_keystore_password;
        String ssl_truststore_location;
        String ssl_truststore_password;
        String auto_offset_reset;
        Long connections_max_idle_ms;
        Boolean enable_auto_commit;
        Boolean exclude_internal_topics;
        Integer fetch_max_bytes;
        Integer max_poll_interval_ms;
        Integer max_poll_records;
        String partition_assignment_strategy;
        Integer receive_buffer_bytes;
        Integer request_timeout_ms;
        String sasl_kerberos_service_name;
        String sasl_mechanism;
        String security_protocol;
        Integer send_buffer_bytes;
        String ssl_enabled_protocols;
        String ssl_keystore_type;
        String ssl_protocol;
        String ssl_provider;
        String ssl_truststore_type;
        Integer auto_commit_interval_ms;
        Boolean check_crcs;
        String client_id;
        Integer fetch_max_wait_ms;
        String interceptor_classes;
        Long metadata_max_age_ms;
        String metric_reporters;
        Integer metrics_num_samples;
        Long metrics_sample_window_ms;
        Long reconnect_backoff_ms;
        Long retry_backoff_ms;
        String sasl_kerberos_kinit_cmd;
        Long sasl_kerberos_min_time_before_relogin;
        Double sasl_kerberos_ticket_renew_jitter;
        Double sasl_kerberos_ticket_renew_window_factor;
        String ssl_cipher_suites;
        String ssl_endpoint_identification_algorithm;
        String ssl_keymanager_algorithm;
        String ssl_secure_random_implementation;
        String ssl_trustmanager_algorithm;

        public Map<String, Object> getConsumerConfig() {
            Map<String, Object> map = new HashMap<>();
            putIfNotNull(map, "bootstrap.servers", bootstrap_servers);
            putIfNotNull(map, "key.deserializer", key_deserializer);
            putIfNotNull(map, "value.deserializer", value_deserializer);
            putIfNotNull(map, "fetch.min.bytes", fetch_min_bytes);
            putIfNotNull(map, "group.id", group_id);
            putIfNotNull(map, "heartbeat.interval.ms", heartbeat_interval_ms);
            putIfNotNull(map, "max.partition.fetch.bytes", max_partition_fetch_bytes);
            putIfNotNull(map, "session.timeout.ms", session_timeout_ms);
            putIfNotNull(map, "ssl.key.password", ssl_key_password);
            putIfNotNull(map, "ssl.keystore.location", ssl_keystore_location);
            putIfNotNull(map, "ssl.keystore.password", ssl_keystore_password);
            putIfNotNull(map, "ssl.truststore.location", ssl_truststore_location);
            putIfNotNull(map, "ssl.truststore.password", ssl_truststore_password);
            putIfNotNull(map, "auto.offset.reset", auto_offset_reset);
            putIfNotNull(map, "connections.max.idle.ms", connections_max_idle_ms);
            putIfNotNull(map, "enable.auto.commit", enable_auto_commit);
            putIfNotNull(map, "exclude.internal.topics", exclude_internal_topics);
            putIfNotNull(map, "fetch.max.bytes", fetch_max_bytes);
            putIfNotNull(map, "max.poll.interval.ms", max_poll_interval_ms);
            putIfNotNull(map, "max.poll.records", max_poll_records);
            putIfNotNull(map, "partition.assignment.strategy", partition_assignment_strategy);
            putIfNotNull(map, "receive.buffer.bytes", receive_buffer_bytes);
            putIfNotNull(map, "request.timeout.ms", request_timeout_ms);
            putIfNotNull(map, "sasl.kerberos.service.name", sasl_kerberos_service_name);
            putIfNotNull(map, "sasl.mechanism", sasl_mechanism);
            putIfNotNull(map, "security.protocol", security_protocol);
            putIfNotNull(map, "send.buffer.bytes", send_buffer_bytes);
            putIfNotNull(map, "ssl.enabled.protocols", ssl_enabled_protocols);
            putIfNotNull(map, "ssl.keystore.type", ssl_keystore_type);
            putIfNotNull(map, "ssl.protocol", ssl_protocol);
            putIfNotNull(map, "ssl.provider", ssl_provider);
            putIfNotNull(map, "ssl.truststore.type", ssl_truststore_type);
            putIfNotNull(map, "auto.commit.interval.ms", auto_commit_interval_ms);
            putIfNotNull(map, "check.crcs", check_crcs);
            putIfNotNull(map, "client.id", client_id);
            putIfNotNull(map, "fetch.max.wait.ms", fetch_max_wait_ms);
            putIfNotNull(map, "interceptor.classes", interceptor_classes);
            putIfNotNull(map, "metadata.max.age.ms", metadata_max_age_ms);
            putIfNotNull(map, "metric.reporters", metric_reporters);
            putIfNotNull(map, "metrics.num.samples", metrics_num_samples);
            putIfNotNull(map, "metrics.sample.window.ms", metrics_sample_window_ms);
            putIfNotNull(map, "reconnect.backoff.ms", reconnect_backoff_ms);
            putIfNotNull(map, "retry.backoff.ms", retry_backoff_ms);
            putIfNotNull(map, "sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
            putIfNotNull(map, "sasl.kerberos.min.time.before.relogin", sasl_kerberos_min_time_before_relogin);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.jitter", sasl_kerberos_ticket_renew_jitter);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.window.factor", sasl_kerberos_ticket_renew_window_factor);
            putIfNotNull(map, "ssl.cipher.suites", ssl_cipher_suites);
            putIfNotNull(map, "ssl.endpoint.identification.algorithm", ssl_endpoint_identification_algorithm);
            putIfNotNull(map, "ssl.keymanager.algorithm", ssl_keymanager_algorithm);
            putIfNotNull(map, "ssl.secure.random.implementation", ssl_secure_random_implementation);
            putIfNotNull(map, "ssl.trustmanager.algorithm", ssl_trustmanager_algorithm);
            return map;
        }
    }

    /**
     * <a href="http://kafka.apache.org/documentation#producerconfigs">Producer configs</a>
     */
    @Data
    @ConfigurationProperties("kafka.producer")
    public static class Producer {
        String bootstrap_servers;
        String key_serializer = "org.apache.kafka.common.serialization.StringSerializer";
        String value_serializer = "org.apache.kafka.common.serialization.StringSerializer";
        String acks;
        Long buffer_memory;
        String compression_type;
        Integer retries;
        String ssl_key_password;
        String ssl_keystore_location;
        String ssl_keystore_password;
        String ssl_truststore_location;
        String ssl_truststore_password;
        Integer batch_size;
        String client_id;
        Long connections_max_idle_ms;
        Long linger_ms;
        Long max_block_ms;
        Integer max_request_size;
        String partitioner_class;
        Integer receive_buffer_bytes;
        Integer request_timeout_ms;
        String sasl_kerberos_service_name;
        String sasl_mechanism;
        String security_protocol;
        Integer send_buffer_bytes;
        String ssl_enabled_protocols;
        String ssl_keystore_type;
        String ssl_protocol;
        String ssl_provider;
        String ssl_truststore_type;
        Integer timeout_ms;
        Boolean block_on_buffer_full;
        String interceptor_classes;
        Integer max_in_flight_requests_per_connection;
        Long metadata_fetch_timeout_ms;
        Long metadata_max_age_ms;
        String metric_reporters;
        Integer metrics_num_samples;
        Long metrics_sample_window_ms;
        Long reconnect_backoff_ms;
        Long retry_backoff_ms;
        String sasl_kerberos_kinit_cmd;
        Long sasl_kerberos_min_time_before_relogin;
        Double sasl_kerberos_ticket_renew_jitter;
        Double sasl_kerberos_ticket_renew_window_factor;
        String ssl_cipher_suites;
        String ssl_endpoint_identification_algorithm;
        String ssl_keymanager_algorithm;
        String ssl_secure_random_implementation;
        String ssl_trustmanager_algorithm;

        public Map<String, Object> getProducerConfig() {
            Map<String, Object> map = new HashMap<>();
            putIfNotNull(map, "bootstrap.servers", bootstrap_servers);
            putIfNotNull(map, "key.serializer", key_serializer);
            putIfNotNull(map, "value.serializer", value_serializer);
            putIfNotNull(map, "acks", acks);
            putIfNotNull(map, "buffer.memory", buffer_memory);
            putIfNotNull(map, "compression.type", compression_type);
            putIfNotNull(map, "retries", retries);
            putIfNotNull(map, "ssl.key.password", ssl_key_password);
            putIfNotNull(map, "ssl.keystore.location", ssl_keystore_location);
            putIfNotNull(map, "ssl.keystore.password", ssl_keystore_password);
            putIfNotNull(map, "ssl.truststore.location", ssl_truststore_location);
            putIfNotNull(map, "ssl.truststore.password", ssl_truststore_password);
            putIfNotNull(map, "batch.size", batch_size);
            putIfNotNull(map, "client.id", client_id);
            putIfNotNull(map, "connections.max.idle.ms", connections_max_idle_ms);
            putIfNotNull(map, "linger.ms", linger_ms);
            putIfNotNull(map, "max.block.ms", max_block_ms);
            putIfNotNull(map, "max.request.size", max_request_size);
            putIfNotNull(map, "partitioner.class", partitioner_class);
            putIfNotNull(map, "receive.buffer.bytes", receive_buffer_bytes);
            putIfNotNull(map, "request.timeout.ms", request_timeout_ms);
            putIfNotNull(map, "sasl.kerberos.service.name", sasl_kerberos_service_name);
            putIfNotNull(map, "sasl.mechanism", sasl_mechanism);
            putIfNotNull(map, "security.protocol", security_protocol);
            putIfNotNull(map, "send.buffer.bytes", send_buffer_bytes);
            putIfNotNull(map, "ssl.enabled.protocols", ssl_enabled_protocols);
            putIfNotNull(map, "ssl.keystore.type", ssl_keystore_type);
            putIfNotNull(map, "ssl.protocol", ssl_protocol);
            putIfNotNull(map, "ssl.provider", ssl_provider);
            putIfNotNull(map, "ssl.truststore.type", ssl_truststore_type);
            putIfNotNull(map, "timeout.ms", timeout_ms);
            putIfNotNull(map, "block.on.buffer.full", block_on_buffer_full);
            putIfNotNull(map, "interceptor.classes", interceptor_classes);
            putIfNotNull(map, "max.in.flight.requests.per.connection", max_in_flight_requests_per_connection);
            putIfNotNull(map, "metadata.fetch.timeout.ms", metadata_fetch_timeout_ms);
            putIfNotNull(map, "metadata.max.age.ms", metadata_max_age_ms);
            putIfNotNull(map, "metric.reporters", metric_reporters);
            putIfNotNull(map, "metrics.num.samples", metrics_num_samples);
            putIfNotNull(map, "metrics.sample.window.ms", metrics_sample_window_ms);
            putIfNotNull(map, "reconnect.backoff.ms", reconnect_backoff_ms);
            putIfNotNull(map, "retry.backoff.ms", retry_backoff_ms);
            putIfNotNull(map, "sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
            putIfNotNull(map, "sasl.kerberos.min.time.before.relogin", sasl_kerberos_min_time_before_relogin);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.jitter", sasl_kerberos_ticket_renew_jitter);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.window.factor", sasl_kerberos_ticket_renew_window_factor);
            putIfNotNull(map, "ssl.cipher.suites", ssl_cipher_suites);
            putIfNotNull(map, "ssl.endpoint.identification.algorithm", ssl_endpoint_identification_algorithm);
            putIfNotNull(map, "ssl.keymanager.algorithm", ssl_keymanager_algorithm);
            putIfNotNull(map, "ssl.secure.random.implementation", ssl_secure_random_implementation);
            putIfNotNull(map, "ssl.trustmanager.algorithm", ssl_trustmanager_algorithm);
            return map;
        }
    }

    private static <K, V> void putIfNotNull(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}