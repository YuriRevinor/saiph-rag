package com.yurirvs.saiph;


import com.mzt.logapi.starter.annotation.EnableLogRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableLogRecord(tenant = "saiph", proxyTargetClass = true)
@SpringBootApplication
public class RAGApplication {

    public static void main(String[] args) {
        SpringApplication.run(RAGApplication.class, args);
    }
}
