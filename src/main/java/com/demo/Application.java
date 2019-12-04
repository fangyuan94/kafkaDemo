package com.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author fangyuan
 */
@SpringBootApplication(exclude={
//        DataSourceAutoConfiguration.class,
//         KafkaAutoConfiguration.class
})
public class Application {

    public static void main(String[] args) {

        SpringApplication.run(Application.class, args);
    }
}
