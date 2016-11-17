package com.accenture.kafka;

import com.accenture.utils.KafkaUtil;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Map;
import java.util.Set;

/**
 * Created by THINK on 2016/11/17.
 */
@Configuration
@ConditionalOnClass({ZkClient.class, KafkaServer.class})
@EnableConfigurationProperties(
        {KafkaProperties.class,
                KafkaProperties.Embedded.class,
                KafkaProperties.Consumer.class,
                KafkaProperties.Producer.class})
@Slf4j
@Data
public class KafkaAutoConfiguration {


    @Autowired(required = false)
    Set<TopicDefine> topicDefines;

    @Bean
    @ConditionalOnMissingBean
    public KafkaDetail kafkaUpdater(
            KafkaConnection kafkaConnection, KafkaUtil kafkaUtil) {

        if (topicDefines != null
                && !topicDefines.isEmpty()) {
            kafkaUtil.createTopics(topicDefines, kafkaConnection);
        }
        return kafkaUtil.getKafkaInfo(kafkaConnection);
    }

    @Configuration
    @ConditionalOnClass(KafkaTemplate.class)
    public static class KafkaTemplateAutoConfiguration {
        @Autowired
        KafkaProperties.Producer producer;
        @Autowired
        KafkaConnection kafkaConnection;

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> producerConfig = producer.getProducerConfig();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnection.brokersAddress);
            return new DefaultKafkaProducerFactory<>(producerConfig);
        }


        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.embedded.enabled", havingValue = "false", matchIfMissing = true)
    public static class NonEmbeddedAutoConfiguration {
        @Autowired
        KafkaProperties kafkaProperties;

        @Bean
        @ConditionalOnProperty(name = {"kafka.zookeeperConnectionString", "kafka.brokersAddress"})
        @ConditionalOnMissingBean
        public KafkaConnection kafka() throws Exception {

            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(kafkaProperties.brokersAddress)
                    .isEmbedded(false)
                    .zookeeperConnectionString(kafkaProperties.zookeeperConnectionString).build();
            return kafkaConnection;
        }

        @Bean
        @ConditionalOnMissingBean
        KafkaUtil productionKafkaUtil() {
            return new KafkaUtil(true);
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.embedded.enabled", havingValue = "true")
    @ConditionalOnClass({EmbeddedZookeeper.class, TestUtils.class})
    public static class EmbeddedAutoConfiguration {


        @Bean
        @ConditionalOnMissingBean
        KafkaUtil devKafkaUtil() {
            return new KafkaUtil(false);
        }

        @Bean
        @ConditionalOnMissingBean
        public KafkaConnection kafkaEmbedded(@Autowired
                                                     KafkaProperties.Embedded embedded) throws Exception {
            KafkaEmbedded kafka = new KafkaEmbedded(embedded.getPort(),
                    embedded.getBrokerCount(),
                    true,
                    embedded.getPartitions(),
                    embedded.getLogFilenamePattern());
            kafka.start();
            String brokersAsString = kafka.getBrokersAsString();
            String zookeeperConnectionString = kafka.getZookeeperConnectionString();
            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(brokersAsString)
                    .isEmbedded(true)
                    .zookeeperConnectionString(zookeeperConnectionString).build();
            return kafkaConnection;
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.consumer.group")
    @ConditionalOnClass(KafkaListenerContainerFactory.class)
    public static class KafkaListenerContainerFactoryAutoConfiguration {

        @Autowired
        KafkaProperties.Consumer consumer;

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
        kafkaListenerContainerFactory(KafkaDetail kafkaDetail) throws Exception {
            ListenerCfbAdapater<String, String> test =
                    new ListenerCfbAdapater(kafkaDetail,
                            consumer.getGroup(),
                            StringDeserializer.class,
                            StringDeserializer.class);
            return test.containerFactory();
        }


        @Bean
        @ConditionalOnProperty(prefix = "kafka.consumer", value = "batch", havingValue = "true")
        public KafkaListenerContainerFactory<?> batchFactory(KafkaDetail kafkaDetail) throws Exception {
            BatchListenerCfbAdapater<String, String> test =
                    new BatchListenerCfbAdapater(kafkaDetail,
                            consumer.getGroup(),
                            StringDeserializer.class,
                            StringDeserializer.class);
            return test.containerFactory();
        }
    }
}
