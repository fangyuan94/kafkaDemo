package com.demo.kafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * @author fangyuan
 */
@Component
@EnableKafka
public class Listeners {

    @KafkaListener(id = "test"
            //,topics = {"springboot_test_topic"}
            ,topicPartitions = {
                 //设置 消费topic springboot_test_topic partition为0数据，1分区起始offset为0 注意：partitions或partitionOffsets属性都可以指定分区，但不能两个都指定。
                @TopicPartition(topic = "springboot_test_topic",partitionOffsets = {
                        @PartitionOffset(partition = "0",initialOffset = "0"),
                        @PartitionOffset(partition = "1",initialOffset = "0"),
                        @PartitionOffset(partition = "2",initialOffset = "0"),
                })
             }
            ,containerFactory = "kafkaListenerContainerFactory"
            ,clientIdPrefix = "_listener"
            ,concurrency = "${listen.concurrency:3}"
            ,idIsGroup = false
            ,groupId ="springboot_test_topic_zj-group"
            ,beanRef = "springboot_test_topic"
            ,autoStartup = "${listen.auto.start:true}"
//            ,errorHandler = "validationErrorHandler"
            ,properties = {"max.poll.interval.ms=3000",
            "max.poll.records=30",
            "enable.auto.commit=false",
            "bootstrap.servers=localhost:9092"
    }
    )
    public void listen(ConsumerRecord<Integer, String> record, Acknowledgment ack,
                       @Header(KafkaHeaders.GROUP_ID) String groupId,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.OFFSET) int offset,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {

        //消息消息
        System.out.println("=======key=========>"+record.key());
        System.out.println("=======value=========>"+record.value());
        //
        System.out.println("=======topic=========>"+topic);
        System.out.println("=======groupId=========>"+groupId);
        System.out.println("=======partition=========>"+partition);
        System.out.println("=======offset=========>"+offset);
        System.out.println("=======timestamp=========>"+timestamp);

        //手动确定 提交offset
        ack.acknowledge();
    }

//
//    @KafkaListener(id = "personInfo"
//            ,topics = {"personTopic"}
//            ,containerFactory = "personListenerContainerFactory"
//            ,clientIdPrefix = ""
//            ,concurrency = "${listen.concurrency:3}"
//            ,idIsGroup = false
//            ,groupId ="personTopic-group"
//            ,beanRef = "personInfo"
//            ,autoStartup = "${listen.auto.start:true}"
////            ,errorHandler = "validationErrorHandler"
//            ,properties = {"max.poll.interval.ms=3000","max.poll.records=30"})
//    public void personInfoListen(
//                        //验证
//                        @Payload @Valid PersonInfo personInfo, Acknowledgment ack,
//                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
//                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
//                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
//
//        System.out.println(personInfo.getName());
//        System.out.println(personInfo.getAge());
//        System.out.println(personInfo.getSex());
//
//    }
//
//
//    //处理验证失败后 处理逻辑
//    @Bean
//    public KafkaListenerErrorHandler validationErrorHandler(){
//
//        return (message,execption)->{
//
//            return null;
//        };
//    }


}
