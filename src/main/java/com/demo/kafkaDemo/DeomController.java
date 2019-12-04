package com.demo.kafkaDemo;

import com.alibaba.fastjson.JSONObject;
import com.demo.kafkaDemo.bean.PersonInfo;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author fangyuan
 */
@RestController
public class DeomController {

    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<Integer, PersonInfo> personKafkaTemplate;

    @Autowired(required=false)
    private JdbcTemplate jdbcTemplate;

    /**
     * 异步发送
     */
    @RequestMapping("sendASyncPersonInfoStr")
    @Transactional(transactionManager = "kafkaTransactionManager",rollbackFor = Exception.class)
    public void sendASyncPersonInfoStr(){

        //要发送内容
        JSONObject j = new JSONObject();

        j.put("name","张三异步");
        j.put("sex","男");
        j.put("age",18);

        Integer key = new Random().nextInt(100);
        //kafka发送消息
        ListenableFuture<SendResult<Integer, String>>  future = kafkaTemplate.send("springboot_test_topic",key,j.toJSONString());


        //异步回调确认
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            //成功调用
            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {

                handleSuccess(integerStringSendResult);
            }
            //失败调用
            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        });

//        int i = 0/0;

        kafkaTemplate.flush();

    }

    /**
     * 同步发送信息
     */
    @RequestMapping("sendSyncPersonInfoStr")
    @Transactional(transactionManager = "kafkaTransactionManager",rollbackFor = Exception.class)
    public void sendSyncPersonInfoStr(){

        JSONObject j = new JSONObject();

        j.put("name","张三同步");
        j.put("sex","男");
        j.put("age",18);

        Integer key = new Random().nextInt(100);
        //kafka发送消息
        try {
            SendResult<Integer, String> integerStringSendResult = kafkaTemplate.send("springboot_test_topic",key,j.toJSONString())
                    .get(5, TimeUnit.SECONDS);

            kafkaTemplate.flush();
            handleSuccess(integerStringSendResult);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void handleSuccess(SendResult<Integer, String> integerStringSendResult){

        ProducerRecord<Integer, String> producerRecord = integerStringSendResult.getProducerRecord();

        //消息消息
        System.out.println("=======key=========>"+producerRecord.key());
        System.out.println("=======value=========>"+producerRecord.value());
        //元数据消息
        RecordMetadata recordMetadata = integerStringSendResult.getRecordMetadata();
        //查看保存成功偏移量
        System.out.println("=======partition=========>"+recordMetadata.partition());
        System.out.println("=======offset=========>"+recordMetadata.offset());
        System.out.println("=======timestamp=========>"+recordMetadata.timestamp());

    }

    /**
     * 异步发送Person信息
     */
    @RequestMapping("sendPersonInfo")
    public void sendPersonInfo(){

        Integer key = new Random().nextInt(100);

        PersonInfo person = PersonInfo.builder().name("丽丽").age(38).sex("女").build();

        final ProducerRecord<Integer,PersonInfo> record = new  ProducerRecord("personTopic",key,person);

        //kafka发送消息
        ListenableFuture<SendResult<Integer, PersonInfo>>  future = personKafkaTemplate.send(record);
        //异步回调确认
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, PersonInfo>>() {

            //成功调用
            @Override
            public void onSuccess(SendResult<Integer, PersonInfo> integerStringSendResult) {

                //发送成功
                //元数据消息
                RecordMetadata recordMetadata = integerStringSendResult.getRecordMetadata();

                //查看保存成功偏移量
                System.out.println("=======partition=========>"+recordMetadata.partition());
                System.out.println("=======offset=========>"+recordMetadata.offset());
                System.out.println("=======timestamp=========>"+recordMetadata.timestamp());
            }
            //失败调用
            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        });

        kafkaTemplate.flush();

    }

    @RequestMapping("personInfoBitch")
    public void personInfo(){

       for (int i=0;i<50;i++){

           PersonInfo person = PersonInfo.builder().name("丽丽"+i).age(38).sex("女").build();

           final ProducerRecord<Integer,PersonInfo> record = new  ProducerRecord("",1,person);

           //kafka发送消息
           personKafkaTemplate.send(record);
       }

    }

    /**
     * 测试kafka事务机制
     */
    @RequestMapping("sendSyncPersonInfoStrByTransaction")
    public void sendSyncPersonInfoStrByTransaction(){
        JSONObject j = new JSONObject();

        j.put("name","张三测试事务");
        j.put("sex","男");
        j.put("age",18);

        Integer key = new Random().nextInt(100);

        /**
         * 如果KafkaTransactionManager正在进行一个事务，则不使用它。而是使用新的“嵌套”事务。
         */
        boolean flag =kafkaTemplate.executeInTransaction(t->{

                t.send("transaction_test_topic",20,j.toJSONString());

                j.put("sex","女");
                t.send("transaction_test_topic",10,j.toJSONString());

//                int i = 0/0;
                return true;

        });

        System.out.println(flag);

    }

    @RequestMapping("sendSyncPersonInfoStrByTransactionZJ")
    @Transactional(transactionManager = "kafkaTransactionManager",rollbackFor = Exception.class)
    public void sendSyncPersonInfoStrByTransactionZJ(){
        JSONObject j = new JSONObject();

        j.put("name","张三测试事务");
        j.put("sex","男");
        j.put("age",18);

        Integer key = new Random().nextInt(100);

        kafkaTemplate.send("transaction_test_topic",20,j.toJSONString());

        j.put("sex","女");
        kafkaTemplate.send("transaction_test_topic",10,j.toJSONString());

//        int i = 0/0;

    }

    /**
     * 测试嵌套事务
     */
    @RequestMapping("sendMessageByNestingTransaction")
    @Transactional(transactionManager = "chainedKafkaTransactionManager",rollbackFor = Exception.class)
    public void sendMessageByNestingTransaction(){

        JSONObject j = new JSONObject();

        j.put("name","张三测试事务");
        j.put("sex","男");
        j.put("age",18);

        kafkaTemplate.send("transaction_test_topic",10,j.toJSONString());

        jdbcTemplate.execute("insert into tx_test(`name`) VALUES('历史')");

    }


}
