package com.demo.kafkaDemo.config;

import com.demo.kafkaDemo.BitchListeners;
import com.demo.kafkaDemo.interceptor.PersonInfoConsumerInterceptor;
import com.demo.kafkaDemo.interceptor.SomeBean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.TopicPartitionOffset;


import java.util.HashMap;
import java.util.Map;

/**
 * 消费者
 * @author fangyaun
 */
@Configuration
public class KafkaConsumerConfigure {

    /**
     * 消费者
     * @return
     */
    @Bean
    public Map<String, Object> consumerConfigs() {

        Map<String, Object> props = new HashMap<>(10);
        //,localhost:9093,localhost:9094
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //设置是否自动提交offset 2.3 版本以后默认为false
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //设置消息拉取最大时间间隔
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,10000);
        //设置一个批次拉取最大消息条数
        props.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG,30);

        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, PersonInfoConsumerInterceptor.class.getName());
        SomeBean someBean = new SomeBean();
        props.put("someBean",someBean);
        //kafka默认分区策略为RangeAssignor 随机分配 可改为RoundRobinAssignor 轮询分配
//        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //添加序列化程序设置到标头
//        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);

        return props;
    }

    /**
     * 消费者工厂类
     * @return
     */
    @Bean
    public ConsumerFactory<Integer,String> consumerFactory() {

        IntegerDeserializer keyDeserializer = new IntegerDeserializer();

        StringDeserializer valueDeserializer = new StringDeserializer();

        ConsumerFactory<Integer,String> consumerFactory =  new DefaultKafkaConsumerFactory(consumerConfigs(),keyDeserializer,valueDeserializer);

        return consumerFactory;
    }

    /**
     * 构建并行消费监听容器 多线程消费
     * @param consumerFactory
     * @return
     */
    @Bean("kafkaListenerContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer,String>> kafkaListenerContainerFactory
    (@Qualifier("consumerFactory") ConsumerFactory<Integer,String> consumerFactory){

        //构建kafka并行消费监听类工厂类 此类通过topic名称创建该topic消费监听
        ConcurrentKafkaListenerContainerFactory<Integer,String> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        //可通过注解的方式进行设置
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
        //
        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckOnError(false);


        //设置ack模型机制 当发生error时 不同处理机制针对与offset有不同处理机制
        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        //设置全局异常处理器
//        concurrentKafkaListenerContainerFactory.setErrorHandler(errorHandler());


//        //设置消费者负载均衡监听
//        concurrentKafkaListenerContainerFactory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
//
//            @Override
//            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
//                // acknowledge any pending Acknowledgments (if using manual acks)
//            }
//
//            @Override
//            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
//                // ...
//
//            }
//
//            @Override
//            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
//                // ...
//
//            }
//        });


//        ConsumerAwareRebalanceListener c = new ConsumerRebalanceListener();

        return concurrentKafkaListenerContainerFactory;
    }


//
//    //构建factory处理该listener
//    @Bean("personListenerContainerFactory")
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, PersonInfo>> personListenerContainerFactory(@Qualifier("consumerConfigs") Map<String,Object> consumerConfigs){
//
//        IntegerDeserializer keyDeserializer = new IntegerDeserializer();
//
//        PersonInfoDeserializer valueDeserializer = new PersonInfoDeserializer();
//
//        ConsumerFactory consumerFactory =  new DefaultKafkaConsumerFactory(consumerConfigs,keyDeserializer,valueDeserializer);
//
//        //构建kafka并行消费监听类工厂类 此类通过topic名称创建该topic消费监听
//        ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//
//        //可通过注解的方式进行设置
//        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
//        //并行数
//        concurrentKafkaListenerContainerFactory.setConcurrency(3);
//        //设置拉取时间超时数
//        concurrentKafkaListenerContainerFactory.getContainerProperties().setPollTimeout(3000);
//
//        //
//        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckOnError(false);
//
//        //设置ack模型机制 当发生error时 不同处理机制针对与offset有不同处理机制
//        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//
//        //批次
//        concurrentKafkaListenerContainerFactory.setBatchListener(true);
//        //设置消息转换器
////        concurrentKafkaListenerContainerFactory.setMessageConverter(new JsonMessageConverter());
//
//        //设置重试机制
////        concurrentKafkaListenerContainerFactory.setRetryTemplate(new RetryTemplate());
//
//        return concurrentKafkaListenerContainerFactory;
//
//    }

//    @Bean
//    public SeekToCurrentErrorHandler errorHandler(){
//        SeekToCurrentErrorHandler handler = new SeekToCurrentErrorHandler();
//        //添加不可重试类别 SeekToCurrentErrorHandler认为某些异常是致命的，因此跳过此类重试
//        handler.addNotRetryableException(IllegalArgumentException.class);
//
//        return handler;
//    }

    /**
     * 构建并行消费监听容器批次处理
     * @param consumerFactory
     * @return
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer,String>> bitchFactory(ConsumerFactory<Integer,String> consumerFactory){

        //构建kafka并行消费监听类工厂类 此类通过topic名称创建该topic消费监听
        ConcurrentKafkaListenerContainerFactory<Integer,String> concurrentKafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();

        //可通过注解的方式进行设置
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
        //并行数
        concurrentKafkaListenerContainerFactory.setConcurrency(3);
        //设置拉取时间超时数
        concurrentKafkaListenerContainerFactory.getContainerProperties().setPollTimeout(3000);
        //是否开启 批次处理
        concurrentKafkaListenerContainerFactory.setBatchListener(true);

        return concurrentKafkaListenerContainerFactory;

    }

    /**
     * 注册bean
     * @return
     */
    @Bean
    public BitchListeners bitchListeners(){

        return new BitchListeners("springboot_test_topic");
    }


    /**
     * 设置并行任务
     * @return
     */
    //@Bean
    public ConcurrentMessageListenerContainer messageListenerContainer(@Qualifier("consumerFactory")  ConsumerFactory<Integer,String> consumerFactory){


        String topicName = "springboot_test_topic";

        //创建topic分区
        TopicPartition topicPartition1 = new TopicPartition(topicName,0);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo1 = new TopicPartitionOffset(topicPartition1,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //创建topic分区
        TopicPartition topicPartition2 = new TopicPartition(topicName,1);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo2 = new TopicPartitionOffset(topicPartition2,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //创建topic分区
        TopicPartition topicPartition3 = new TopicPartition(topicName,2);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo3 = new TopicPartitionOffset(topicPartition3,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //ConsumerFactory设置topic的主题和分区
        //容器配置
        ContainerProperties containerProperties = new ContainerProperties(tpo1,tpo2,tpo3);

        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setClientId("springboot_test_topic_concurrent_id");
        containerProperties.setGroupId("springboot_test_topic_concurrent_group");
        containerProperties.setAckTime(6000);
        containerProperties.getKafkaConsumerProperties().setProperty("enable.auto.commit","false");
        containerProperties.setPollTimeout(3000);
        containerProperties.setAckOnError(false);

        //绑定message 消息监听 自定提交 单条消费 存在丢失数据 以及重复消费的问题
        containerProperties.setMessageListener((AcknowledgingMessageListener<Integer, String>) (consumerRecord, acknowledgment) -> {

            System.out.println("partition============>"+consumerRecord.partition());
            System.out.println("offset============>"+consumerRecord.offset());
            System.out.println("key============>"+consumerRecord.key());
            System.out.println("value============>"+consumerRecord.value());

            //确定消费完成 commit offset
            acknowledgment.acknowledge();
        });

        containerProperties.getTopicPartitionsToAssign();


        ConcurrentMessageListenerContainer<Integer,String> cmlc = new ConcurrentMessageListenerContainer(consumerFactory,containerProperties);


        //是否设置虽容器自动启动
        cmlc.setAutoStartup(true);
        cmlc.setBeanName("concurrentMessageListenerContainer");

        //设置并发数
        cmlc.setConcurrency(3);
        //启动消费监听
        cmlc.start();

        return cmlc;
    }

    /**
     * 配置单点消费者容器
     * @return
     */
    //@Bean
    public KafkaMessageListenerContainer messageListenerContainer1(@Qualifier("consumerFactory") ConsumerFactory<Integer,String> consumerFactory){

        String topicName = "springboot_test_topic";

        //创建topic分区
        TopicPartition topicPartition1 = new TopicPartition(topicName,0);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo1 = new TopicPartitionOffset(topicPartition1,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //创建topic分区
        TopicPartition topicPartition2 = new TopicPartition(topicName,1);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo2 = new TopicPartitionOffset(topicPartition2,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //创建topic分区
        TopicPartition topicPartition3 = new TopicPartition(topicName,2);

        //设置需要消费的分区偏移量
        TopicPartitionOffset tpo3 = new TopicPartitionOffset(topicPartition3,0L,TopicPartitionOffset.SeekPosition.BEGINNING);

        //ConsumerFactory设置topic的主题和分区
        //容器配置
        ContainerProperties containerProperties = new ContainerProperties(tpo1,tpo2,tpo3);
        /**
         * 指定
         */
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setClientId("springboot_test_topic_id");
        containerProperties.setGroupId("springboot_test_topic_group");
        containerProperties.setAckTime(6000);
        containerProperties.getKafkaConsumerProperties().setProperty("enable.auto.commit","false");
        containerProperties.setPollTimeout(3000);
        containerProperties.setAckOnError(false);

        //绑定message 消息监听 手动提交 单条消费
        containerProperties.setMessageListener((AcknowledgingMessageListener<Integer, String>) (consumerRecord, acknowledgment) -> {

            System.out.println("partition============>"+consumerRecord.partition());
            System.out.println("offset============>"+consumerRecord.offset());
            System.out.println("key============>"+consumerRecord.key());
            System.out.println("value============>"+consumerRecord.value());
            //确定消费完成 commit offset
            acknowledgment.acknowledge();
        });


        //构建kafka消费者监听容器
        KafkaMessageListenerContainer<Integer,String>  kafkaMessageListenerContainer =
                new KafkaMessageListenerContainer<Integer,String>(consumerFactory,containerProperties);

        //启动消费监听
        kafkaMessageListenerContainer.start();

        return kafkaMessageListenerContainer;
    }

//    /**
//     * 配置消费者容器
//     * @return
//     */
//    @Bean
//    public KafkaMessageListenerContainer messageListenerContainer( ConsumerFactory<Integer,String> consumerFactory){
//
//        //ConsumerFactory设置topic的主题和分区
//        //容器配置
//        ContainerProperties containerProperties = new ContainerProperties("springboot_test_topic");
//
//        //绑定message 消息监听 自定提交 单条消费 存在丢失数据 以及重复消费的问题
//        containerProperties.setMessageListener(new MessageListener<Integer,String>() {
//            @Override
//            public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
//
//                System.out.println("key============>"+consumerRecord.key());
//
//                System.out.println("value============>"+consumerRecord.value());
//            }
//
//        });
//
//        //设置手动提交的ack机制
//        /**
//         * RECORD：处理完记录后，当侦听器返回时，提交偏移量。
//         *
//         * BATCH：处理完所返回的所有记录后，提交偏移量poll()。
//         *
//         * TIME：poll()只要ackTime已超过自最后一次提交以来的时间，就处理完所返回的所有记录后就提交偏移量。
//         *
//         * COUNT：poll()只要ackCount自上次提交以来就已收到记录，则在处理了所返回的所有记录时提交偏移量。
//         *
//         * COUNT_TIME：与TIME和类似COUNT，但是如果任一条件为则执行提交true。
//         *
//         * MANUAL：消息侦听器acknowledge()对Acknowledgment。之后，将应用相同的语义BATCH。
//         *
//         * MANUAL_IMMEDIATE：当Acknowledgment.acknowledge()侦听器调用该方法时，立即提交偏移量。
//         *
//         *MANUAL，和MANUAL_IMMEDIATE要求监听对象为AcknowledgingMessageListener或BatchAcknowledgingMessageListener
//         *
//         */
//
//        //绑定message 消息监听 手动提交 单条消费
//        containerProperties.setMessageListener(new AcknowledgingMessageListener<Integer, String>() {
//            @Override
//            public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
//
//                //消费消息
//
//                System.out.println("key============>"+consumerRecord.key());
//
//                System.out.println("value============>"+consumerRecord.value());
//
//                //手动提交 表明已收到消息
//                acknowledgment.acknowledge();
//                //
//            }
//        });
//
//        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
//
//        //批次消费 自动提交 存在丢失数据 以及重复消费的问题
//        containerProperties.setMessageListener(new BatchMessageListener<Integer, String>(){
//
//            @Override
//            public void onMessage(List<ConsumerRecord<Integer, String>> consumerRecords) {
//
//                //重复消费
//                consumerRecords.forEach(consumerRecord->{
//
//                    System.out.println("key============>"+consumerRecord.key());
//
//                    System.out.println("value============>"+consumerRecord.value());
//                });
//            }
//        });
//
//        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//
//        //批次消费 手动提交
//        containerProperties.setMessageListener(new BatchAcknowledgingMessageListener<Integer, String>() {
//            @Override
//            public void onMessage(List<ConsumerRecord<Integer, String>> consumerRecords, Acknowledgment acknowledgment) {
//
//                consumerRecords.forEach(consumerRecord->{
//
//                    System.out.println("key============>"+consumerRecord.key());
//
//                    System.out.println("value============>"+consumerRecord.value());
//                });
//
//            }
//        });
//
//        //创建topic分区
//        TopicPartition topicPartition = new TopicPartition("test",1);
//
//        //设置需要消费的分区偏移量
//        TopicPartitionOffset tpo = new TopicPartitionOffset(topicPartition,1L,TopicPartitionOffset.SeekPosition.BEGINNING);
//
//        //构建kafka消费者监听容器(单条)
//        KafkaMessageListenerContainer<Integer,String>  kafkaMessageListenerContainer =
//                new KafkaMessageListenerContainer<Integer,String>(consumerFactory,containerProperties);
//
//
//        return kafkaMessageListenerContainer;
//    }

}
