package com.demo.kafkaDemo.partitioner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;


/**
 *
 * 自定义分区器
 */
public class DefaultPartitioner implements Partitioner {

    /**
     *  线程安全topic计数器容器
     */
    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

    /**
     * 获取配置定义属性
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {}

    /**
     * 核心分区方法
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //获取该topic下partition
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null) {
            //没有指定key的话 按照轮询算法来算出partition

            //获取下个
            int nextValue = nextValue(topic);

            //获取有效的分区数
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);

            if (availablePartitions.size() > 0) {
                //算出该topic下一个执行的partition
                // 例如A topic存在有效的0,1,2 产生1-6 6条数据 那么该算法保证0partition会保存1,4数据 1partition会保存2,5 2partition会保存3,6
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {

                // 如果没有分区数 对nextValue进行hash取值 % 分区数 随机分区
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // 如果指定key 对key进行hash取值 % 分区数
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    private int nextValue(String topic) {
        //获取当前topic以被执行的次数
        AtomicInteger counter = topicCounterMap.get(topic);
        if (null == counter) {
            //初始化
            counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());
            AtomicInteger currentCounter = topicCounterMap.putIfAbsent(topic, counter);

            if (currentCounter != null) {
                counter = currentCounter;
            }
        }
        return counter.getAndIncrement();
    }

    @Override
    public void close() {}

}

