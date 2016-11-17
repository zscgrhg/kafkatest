package com.accenture.kafka;

import kafka.server.KafkaServer;
import lombok.Builder;
import lombok.Singular;

import java.util.List;

/**
 * Created by THINK on 2016/11/16.
 */
@Builder
public class KafkaConnection {
    public final String zookeeperConnectionString;
    public final String brokersAddress;
    public final boolean isEmbedded;
}
