package com.accenture.kafka;

import lombok.Builder;

import java.util.Map;

@Builder
public class KafkaDetail{
    public final KafkaConnection kafkaConnection;
    public final int maxPartitions;
    public final Map<String, Integer> topicPartitions;
}
