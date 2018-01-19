package com.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: fansy
 * @Time: 2018/1/18 14:46
 * @Email: fansy1990@foxmail.com
 */
@Configuration
public class ElasticSearchConfig {
    @Value("${es.nodes}")
    private String nodes;
    @Value("${es.port}")
    private String port;
    @Value("${es.mapping.date.rich}")
    private String dataRich;
    @Value("${es.resource}")
    private String resource;

    public String getCheckInterval() {
        return checkInterval;
    }

    public void setCheckInterval(String checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Value("${elasticsearch2hive.job.check.interval}")
    private String checkInterval;
    public String getHiveTable() {
        return hiveTable;
    }

    public void setHiveTable(String hiveTable) {
        this.hiveTable = hiveTable;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    @Value("${es.hive.table}")

    private String hiveTable;
    @Value("${es.columns}")
    private String columns;

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDataRich() {
        return dataRich;
    }

    public void setDataRich(String dataRich) {
        this.dataRich = dataRich;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }
}
