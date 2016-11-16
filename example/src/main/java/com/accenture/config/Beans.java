package com.accenture.config;

import com.accenture.kafka.KafkaConnection;
import com.accenture.kafka.KafkaDetail;
import com.accenture.kafka.TopicDefine;
import com.accenture.utils.KafkaUtil;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.Set;

/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
@EnableKafka
@Slf4j
public class Beans {

    @Autowired(required = false)
    Set<TopicDefine> topicDefines;


    @Bean
    KafkaUtil productionKafkaUtil() {
        return new KafkaUtil(true);
    }


    @Bean
    public KafkaDetail updateKafka(
            KafkaConnection kafkaConnection, KafkaUtil kafkaUtil) {
        ZkUtils zkUtils = new ZkUtils(new ZkClient(kafkaConnection.zookeeperConnectionString, 6000, 6000,
                ZKStringSerializer$.MODULE$), null, false);
        if (topicDefines != null && !topicDefines.isEmpty()) {

            try {
                kafkaUtil.createTopics(topicDefines, zkUtils);
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }

        return kafkaUtil.getKafkaInfo(zkUtils, kafkaConnection);
    }


}
