package com.accenture.config;

import com.accenture.kafka.TopicDefine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.Properties;

/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
public class KafkaTopics {
    @Bean
    TopicDefine topicDefine1() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("abcde")
                        .partitions(3)
                        .replicationFactor(3)
                        .topicConfig(new Properties())
                        .rackAwareMode(null)
                        .build();
        return topicDefine;
    }

    @Bean
    TopicDefine topicDefine2() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("qwert")
                        .partitions(30)
                        .replicationFactor(3)
                        .topicConfig(new Properties())
                        .rackAwareMode(null)
                        .build();
        return topicDefine;
    }
}
