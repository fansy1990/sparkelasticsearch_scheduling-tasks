package com.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Created by fansy on 2018/1/18.
 */
@Configuration
public class DemoJobConfig {
    @Value("demo.job.cron")
    private String cron;

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
