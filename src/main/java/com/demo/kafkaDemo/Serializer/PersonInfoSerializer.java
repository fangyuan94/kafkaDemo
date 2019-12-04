package com.demo.kafkaDemo.Serializer;

import com.demo.kafkaDemo.bean.PersonInfo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 自定义序列化器
 * @author fangyaun
 */
public class PersonInfoSerializer implements Serializer<PersonInfo> {

    @Override
    public byte[] serialize(String topic, PersonInfo personInfo) {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);

        try {
            if (!(personInfo instanceof Serializable)) {
                throw new IllegalArgumentException(this.getClass().getSimpleName() + " requires a Serializable payload but received an object of type [" + personInfo.getClass().getName() + "]");
            } else {
                ObjectOutputStream objectOutputStream = null;
                try {
                    objectOutputStream = new ObjectOutputStream(outputStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                objectOutputStream.writeObject(personInfo);
                objectOutputStream.flush();
            }
            return outputStream.toByteArray();
        } catch (Throwable ex) {
            throw new SerializationException("Can't serialize data [" + personInfo + "] for topic [" + topic + "]", ex);
        }
    }

}
