package com.demo.kafkaDemo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 *
 * @author fangyuan
 * 数据配置
 */
@Configuration
public class DataSourceConfigure {


    /**
     *
     * @param dataSource
     * @return
     */
    @Bean
    public DataSourceTransactionManager dstm(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

}
