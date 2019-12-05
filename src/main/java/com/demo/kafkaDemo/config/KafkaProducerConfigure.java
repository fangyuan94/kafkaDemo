package com.demo.kafkaDemo.config;

import com.demo.kafkaDemo.bean.PersonInfo;
import com.demo.kafkaDemo.Serializer.PersonInfoSerializer;
import com.demo.kafkaDemo.interceptor.PersonInfoProducerInterceptor;
import com.demo.kafkaDemo.interceptor.SomeBean;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.*;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fangyuan
 */
@Configuration
public class KafkaProducerConfigure {

    /**
     *构建
     * @return
     */
    @Bean
    public Map<String, Object> producerConfigs() {


        Map<String, Object> props = new HashMap<>(10);

        //,localhost:9093,localhost:9094
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        //指定拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PersonInfoProducerInterceptor.class.getName());
        SomeBean someBean = new SomeBean();
        props.put("someBean",someBean);
        /**
         * 自定义分区器 如果不指定则为默认分区器 DefaultPartitioner
         */
        props.put("partitioner.class","com.demo.kafkaDemo.partitioner.MyPartitioner");

        return props;
    }

    /**
     * 构建生产者工厂类
     */
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {

        Map<String, Object> configs = producerConfigs();

        DefaultKafkaProducerFactory<Integer,String>  producerFactory =  new DefaultKafkaProducerFactory(configs,new IntegerSerializer(),new StringSerializer());

        //设置事务Id前缀
        producerFactory.setTransactionIdPrefix("tx-");
        return producerFactory;
    }


    /**
     * 构建spring KafkaTemplate
     * @return
     */
    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<Integer, String>(producerFactory());
    }

    /**
     * 构建spring KafkaTemplate
     * @return
     */
    @Bean
    public KafkaTemplate<Integer,PersonInfo> personKafkaTemplate() {

        IntegerSerializer keySerializer = new IntegerSerializer();

        PersonInfoSerializer valueSerializer = new PersonInfoSerializer();

        ProducerFactory<Integer,PersonInfo>  producerPersonFactory =  new DefaultKafkaProducerFactory(producerConfigs(),keySerializer,valueSerializer);

        KafkaTemplate<Integer,PersonInfo> kafkaTemplate = new KafkaTemplate(producerPersonFactory);

        return kafkaTemplate;
    }


    @Bean
    public KafkaTransactionManager<Integer, String> kafkaTransactionManager(ProducerFactory<Integer, String> producerFactory) {

        return new KafkaTransactionManager<>(producerFactory);
    }


    /**
     * 构建嵌套事务组件
     */
    @Bean
    public ChainedKafkaTransactionManager<Integer, String> chainedKafkaTransactionManager(
            KafkaTransactionManager<Integer, String> kafkaTransactionManager,
            DataSourceTransactionManager dstm) {

        return new ChainedKafkaTransactionManager<>(kafkaTransactionManager, dstm);
    }

}
