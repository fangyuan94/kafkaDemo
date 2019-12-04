package com.demo.kafkaDemo.Serializer;

import com.demo.kafkaDemo.bean.PersonInfo;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.core.serializer.support.SerializationFailedException;

import java.io.ByteArrayInputStream;

/**
 * 自定义反序列化器
 * @author fangyaun
 */
public class PersonInfoDeserializer implements Deserializer<PersonInfo> {

    @Override
    public PersonInfo deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        try {
            ConfigurableObjectInputStream objectInputStream = new ConfigurableObjectInputStream(inputStream,null);
            return (PersonInfo) objectInputStream.readObject();

        } catch (Throwable var4) {
            throw new SerializationFailedException("Failed to deserialize payload for topic [" + topic + "]" , var4);
        }

    }

}
