package com.demo.kafkaDemo.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 *
 *@author fangyuan
 *在KafkaConsumerConfigure,KafkaProducerConfigure中对应配置之前执行
 * 构建kafka topic如果topic已经存在，忽略Bean
 */
@Configuration
@AutoConfigureBefore(value = {KafkaConsumerConfigure.class, KafkaProducerConfigure.class})
public class TopicConfigure {


    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>(1);
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaAdmin admin = new KafkaAdmin(props);

        admin.setFatalIfBrokerNotAvailable(false);

//        AdminClient kafkaAdminClient = AdminClient.create(props);

//        kafkaAdminClient.createTopics()

        return admin;
    }


    @Bean
    public NewTopic createSpringboot_test_topic(){

        return TopicBuilder.name("springboot_test_topic")
                .partitions(3)
                .replicas(1)
                //设置是否日志清除策略 compact 压缩 delete
                .compact()
                .build();
    }

    @Bean
    public NewTopic createAuthorityDockingTopic(){

        return TopicBuilder.name("transaction_test_topic")
                .partitions(3)
                .replicas(1)
                //设置是否日志清除策略 compact 压缩 delete
                .compact()
                .build();
    }


    /**
     *
     * @return
     */
    @Bean
    public NewTopic createPersonTopic(){


//        return TopicBuilder.name("personTopic")
//                //手动分配不同partition的副本分布情况
//                .assignReplicas(0, Arrays.asList(0,1,2))
//                .assignReplicas(1, Arrays.asList(1,2,0))
//                .assignReplicas(2, Arrays.asList(2,0,1))
//                //设置是否日志清除策略 compact 压缩 delete
//                .compact()
//                .build();
        return TopicBuilder.name("personTopic")
                //手动分配不同partition的副本分布情况
                .partitions(3)
                .replicas(1)
                //设置是否日志清除策略 compact 压缩 delete
                .compact()
                .build();
    }
}
