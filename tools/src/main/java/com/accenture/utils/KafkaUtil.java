package com.accenture.utils;

import com.accenture.kafka.KafkaConnection;
import com.accenture.kafka.KafkaDetail;
import com.accenture.kafka.TopicDefine;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.requests.MetadataResponse;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by THINK on 2016/11/16.
 */
public class KafkaUtil {
    private final boolean isProduction;

    public KafkaUtil(final boolean isProduction) {
        this.isProduction = isProduction;
    }


    private Set<String> getExistTopics(ZkUtils zkUtils) {
        Seq<String> allTopics = zkUtils.getAllTopics();
        List<String> t = JavaConversions.seqAsJavaList(allTopics);
        Set<String> topics = new HashSet<>();
        topics.addAll(t);
        return topics;
    }

    public void createTopics(Set<TopicDefine> topicDefines, ZkUtils zkUtils) {
        Set<String> existTopics = getExistTopics(zkUtils);
        for (TopicDefine topicDefine : topicDefines) {
            if (existTopics.contains(topicDefine.topic)) {
                if (!isProduction) {
                    AdminUtils.deleteTopic(zkUtils, topicDefine.topic);
                } else {
                    continue;
                }
            }
            try {
                AdminUtils.createTopic(zkUtils,
                        topicDefine.topic,
                        topicDefine.partitions,
                        topicDefine.replicationFactor,
                        topicDefine.topicConfig,
                        topicDefine.rackAwareMode);
            } catch (TopicExistsException e) {
                // no-op
            }
        }
    }

    public KafkaDetail getKafkaInfo(ZkUtils zkUtils, KafkaConnection kafkaConnection) {
        Set<String> topics = getExistTopics(zkUtils);
        scala.collection.Set<MetadataResponse.TopicMetadata> topicMetadataSet
                = AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkUtils);
        Set<MetadataResponse.TopicMetadata> topicMetadatas = JavaConversions.setAsJavaSet(topicMetadataSet);
        int max = 0;
        Map<String, Integer> topicPartitions = new HashMap<>();
        for (MetadataResponse.TopicMetadata topicMetadata : topicMetadatas) {
            String topic = topicMetadata.topic();
            List<MetadataResponse.PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
            int size = partitionMetadatas.size();
            max = Math.max(max, size);
            topicPartitions.put(topic, size);
        }
        KafkaDetail kafkaDetail = KafkaDetail.builder().kafkaConnection(kafkaConnection)
                .topicPartitions(Collections.unmodifiableMap(topicPartitions))
                .maxPartitions(max).build();
        return kafkaDetail;
    }
}
