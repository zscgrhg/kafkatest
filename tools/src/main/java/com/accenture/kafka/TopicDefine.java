package com.accenture.kafka;

import kafka.admin.RackAwareMode;
import lombok.Builder;
import lombok.EqualsAndHashCode;

import java.util.Properties;

/**
 * Created by THINK on 2016/11/16.
 */

@Builder
@EqualsAndHashCode(of={"topic"})
public class TopicDefine {
    public final String topic;
    public final int partitions;
    public final int replicationFactor;
    public final Properties topicConfig;
    public final RackAwareMode rackAwareMode;
}
