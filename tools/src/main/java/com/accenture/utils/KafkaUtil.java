package com.accenture.utils;

import com.accenture.kafka.KafkaConnection;
import com.accenture.kafka.KafkaDetail;
import com.accenture.kafka.TopicDefine;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.requests.MetadataResponse;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by THINK on 2016/11/16.
 */
public class KafkaUtil {
    private final boolean isEmbedded;

    public KafkaUtil(final boolean isEmbedded) {
        this.isEmbedded = isEmbedded;
    }


    private Set<String> fetchExistTopics(ZkUtils zkUtils) {
        Seq<String> allTopics = zkUtils.getAllTopics();
        List<String> t = JavaConversions.seqAsJavaList(allTopics);
        Set<String> topics = new HashSet<>();
        topics.addAll(t);
        return topics;
    }

    public void createTopics(Set<TopicDefine> topicDefines, KafkaConnection kafkaConnection) {
        ZkUtils zkUtils = getAvaliableZkUtils(kafkaConnection);
        Set<String> existTopics = fetchExistTopics(zkUtils);
        for (TopicDefine topicDefine : topicDefines) {
            if (existTopics.contains(topicDefine.topic)) {
                if (!isEmbedded && kafkaConnection.isEmbedded) {
                    AdminUtils.deleteTopic(zkUtils, topicDefine.topic);
                } else {
                    continue;
                }
            }
            Properties p = new Properties();
            p.putAll(topicDefine.topicConfigs);
            AdminUtils.createTopic(zkUtils,
                    topicDefine.topic,
                    Math.max(topicDefine.partitions, 1),
                    Math.max(topicDefine.replicationFactor, 1),
                    p,
                    topicDefine.rackAwareMode);
        }
    }

    public KafkaDetail getKafkaInfo(KafkaConnection kafkaConnection) {
        ZkUtils zkUtils = getAvaliableZkUtils(kafkaConnection);
        Set<String> topics = fetchExistTopics(zkUtils);
        scala.collection.Set<MetadataResponse.TopicMetadata> topicMetadataSet
                = AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkUtils);
        Set<MetadataResponse.TopicMetadata> topicMetadatas = JavaConversions.setAsJavaSet(topicMetadataSet);
        int max = 1;
        Map<String, Integer> topicPartitions = new HashMap<>();
        for (MetadataResponse.TopicMetadata topicMetadata : topicMetadatas) {
            String topic = topicMetadata.topic();
            List<MetadataResponse.PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
            int size = partitionMetadatas.size();
            max = Math.max(max, size);
            topicPartitions.put(topic, size);
        }
        KafkaDetail kafkaDetail = KafkaDetail.builder().kafkaConnection(kafkaConnection)
                .topicPartitions(topicPartitions)
                .metadatas(topicMetadatas)
                .maxPartitions(max).build();
        return kafkaDetail;
    }

    private ZkUtils getAvaliableZkUtils(KafkaConnection kafkaConnection) {
        ZkUtils zkUtils = new ZkUtils(new ZkClient(kafkaConnection.zookeeperConnectionString, 60 * 1000, 60 * 1000,
                ZKStringSerializer$.MODULE$), null, false);
        return zkUtils;
    }
}
