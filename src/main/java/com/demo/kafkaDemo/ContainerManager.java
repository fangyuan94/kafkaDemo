package com.demo.kafkaDemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 容器管理类
 */
@Component
public class ContainerManager {

    @Autowired(required=false)
    private KafkaListenerEndpointRegistry registry;
//
    public void test(){

        //停止id名为consumer2为监听容器
        registry.getListenerContainer("consumer2").stop();

        //启动监听容器
        registry.getListenerContainer("consumer2").start();

        //获取托管容器的集合 但不包含被声明为Bean的容器
        Collection<MessageListenerContainer> cs =  registry.getListenerContainers();

        //获取所有容器的集合 包括由注册表管理的容器和声明为bean的容器
        Collection<MessageListenerContainer> containers = registry.getAllListenerContainers();

        //获取所有容器ids
        Set<String> containerIds =  registry.getListenerContainerIds();
//        KafkaListenerEndpoint endpoint = null;

        //注册新的监听
//        registry.registerListenerContainer(endpoint,kafkaListenerContainerFactory);

    }
}
