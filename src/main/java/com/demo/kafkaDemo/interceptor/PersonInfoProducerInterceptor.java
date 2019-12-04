package com.demo.kafkaDemo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器
 * @author fangyuan
 */
public class PersonInfoProducerInterceptor implements ProducerInterceptor<Integer,String> {

    private SomeBean someBean;

    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {

        someBean.execute("producer interceptor");

        return record;
    }

    /**
     * 被手动提交后执行代码
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

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
