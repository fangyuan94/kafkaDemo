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

        //获取spring容器中bean名为consumer2的ListenerContainer
        MessageListenerContainer mlc = registry.getListenerContainer("consumer2");
        mlc.pause();
        //获取托管容器的集合 但不包含被声明为Bean的容器
        Collection<MessageListenerContainer> cs =  registry.getListenerContainers();

        //获取所有容器的集合 包括由注册表管理的容器和声明为bean的容器
        Collection<MessageListenerContainer> containers = registry.getAllListenerContainers();

        //获取所有容器ids
        Set<String> containerIds =  registry.getListenerContainerIds();
        //注册新的监听
//        registry.registerListenerContainer(endpoint,kafkaListenerContainerFactory);


        //是否随spirng自启动
        registry.isAutoStartup();

        //
        registry.isRunning();

    }
}
