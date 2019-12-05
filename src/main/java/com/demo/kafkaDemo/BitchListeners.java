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
 * @author fangyuan
 */
public class BitchListeners {

    //
    private final String topic;

    public BitchListeners(String topic) {
        this.topic = topic;
    }

    /**
     * 批处理
     *
     */
    @KafkaListener(id = "bitchConsumer"
            , topics = {"__listener.topic"}
            , containerFactory = "bitchFactory"
//            , clientIdPrefix = ""
            , idIsGroup = false
            , concurrency = "${listen.concurrency:3}"
            , groupId = "bitchConsumer-group"
            , beanRef = "bitchConsumer_"
            , autoStartup = "${listen.auto.start:true}"
            , properties = {"max.poll.interval.ms=3000", "max.poll.records=30"})
    public void listen(List<ConsumerRecord<Integer, String>> records, Acknowledgment ack,
                       @Header(KafkaHeaders.GROUP_ID) String groupId,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

        System.out.println("该批次拉取消息数====================>"+records.size());
        //遍历消息
        records.forEach(record->{
            //消息消息
            System.out.println("=======key=========>"+record.key());
            System.out.println("=======value=========>"+record.value());
            System.out.println("=======topic=========>"+record.topic());

        });
        System.out.println("=======groupId=========>"+groupId);
        System.out.println("=======partition=========>"+partition);

        //手动确定 提交offset
        ack.acknowledge();
    }
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

    /**
     *
     * @param list
     * @param ack
     */
    public void listCRsAck(List<ConsumerRecord<Integer, String>> list, Acknowledgment ack) {
    }


}