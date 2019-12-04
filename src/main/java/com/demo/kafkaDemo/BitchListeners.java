package com.demo.kafkaDemo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 使用SpEL表达式支持特殊的标记：__listener
 */
//通过bean注入
public class BitchListeners {

    private final String topic;

    public BitchListeners(String topic) {
        this.topic = topic;
    }

    /**
     * 批处理
     *
     * @param datas
     * @param key
     * @param partition
     * @param topic
     * @param offsets
     */
//    @KafkaListener(id = "bitchConsumer1"
//            , topics = {"__listener.topic"}
//            , containerFactory = "bitchFactory"
//            , clientIdPrefix = ""
//            , idIsGroup = false
//            , concurrency = "${listen.concurrency:3}"
//            , groupId = "bitchConsumer-1"
//            , beanRef = "bitchConsumer1"
//            , autoStartup = "${listen.auto.start:true}"
//            , properties = {"max.poll.interval.ms=3000", "max.poll.records=30"})
//    public void listen(List<String> datas,
//                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) Integer key,
//                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
//                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//                       @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
//
//    }
//
//    /**
//     * @param messages
//     */
//    @
//    KafkaListener(id = "bitchConsumer2"
//            , topics = {"__listener.topic"}
//            , containerFactory = "bitchFactory"
//            , idIsGroup = false
//            , concurrency = "${listen.concurrency:3}"
//            , groupId = "bitchConsumer-1")
//    public void listMsg(List<Message<String>> messages) {
//        //
//    }

    /**
     * 添加批次ack确认机制
     *
     * @param messages
     * @param ack
     */
    public void listMsgAck(List<Message<String>> messages, Acknowledgment ack) {
        //
    }

    /**
     * @param messages
     * @param ack
     * @param consumer
     */

    public void listMsgAckConsumer(List<Message<String>> messages, Acknowledgment ack,
                                   Consumer<Integer, String> consumer) {
        //
    }

    /**
     * 如果使用使用ConsumerRecord 接收消息方法入参数只能为ConsumerRecord (如果启用ack机制可以包含Acknowledgment)
     * @param list
     */
    public void listCRs(List<ConsumerRecord<Integer, String>> list) {
    }

    public void listCRsAck(List<ConsumerRecord<Integer, String>> list, Acknowledgment ack) {
    }


}