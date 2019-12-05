package com.demo.kafkaDemo.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * @author fangyuan
 * 自定义分区器
 */
public class MyPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //编写自己分区规则
        int numPartitions =cluster.partitionCountForTopic(topic);

        //我这里随便编写了个
        // 让所有数据都写到0分区中
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
