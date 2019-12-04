package com.demo.kafkaDemo.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * 自定义消费者拦截器
 * @author fangyuan
 */
public class PersonInfoConsumerInterceptor implements ConsumerInterceptor<Integer,String> {

    private SomeBean someBean;

    @Override
    public ConsumerRecords<Integer, String> onConsume(ConsumerRecords<Integer, String> records) {

        someBean.execute("consumer interceptor");

        return records;
    }

    /**
     * 提交offset 时需要处理的
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    /**
     * 关闭需要执行代码
     */
    @Override
    public void close() {

    }

    /**
     * 可以从配置文件获取一些配置信息
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

        someBean = (SomeBean) configs.get("someBean");
    }
}
