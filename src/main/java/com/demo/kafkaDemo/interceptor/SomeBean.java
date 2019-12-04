package com.demo.kafkaDemo.interceptor;

/**
 *拦截器中依赖Bean
 * @author fangyuan
 */
public class SomeBean {

    public  void execute(String nr){
        System.out.println(nr);

    }
}
