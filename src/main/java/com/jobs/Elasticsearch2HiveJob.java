package com.jobs;

import com.config.ElasticSearchConfig;
import com.config.SparkConfig;
import com.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.rest.SparkEngine2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 同步Elasticsearch 数据到Hive任务
 * Created by fansy on 2018/1/18.
 */
@Component
public class Elasticsearch2HiveJob {
    private static final Logger log = LoggerFactory.getLogger(Elasticsearch2HiveJob.class);
    @Autowired
    private SparkConfig sparkConfig;
    @Autowired
    private ElasticSearchConfig elasticSearchConfig;

    @Autowired
    private SparkEngine2 sparkEngine;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    @Scheduled(cron = "${elasticsearch2hive.job.cron}")
    public void synEs2Hive() {
        log.info("Start time : {}", dateFormat.format(new Date()));
        String appId = sparkEngine.submit(
                SparkUtils.getSparkConf(sparkConfig.getMaster(),sparkConfig.getExecutorCores(),
                        sparkConfig.getDeployMode(),sparkConfig.getAppResource(),
                        sparkConfig.getExecutorMemory(),sparkConfig.getCoresMax(),
                        sparkConfig.getSupervise(),sparkConfig.getExtraClassPath()),
                sparkConfig.getAppResource(),
                sparkConfig.getMainClass(),
                //esNodeIp,esNodePort,esTable,hiveTable,columns
                elasticSearchConfig.getNodes(),
                elasticSearchConfig.getPort(),
                elasticSearchConfig.getResource(),
                elasticSearchConfig.getHiveTable()+"_"+System.currentTimeMillis(),
                elasticSearchConfig.getColumns()
        );

        log.info("monitoring app :{}",appId);
        sparkEngine.monitory(SparkUtils.getClient(sparkConfig.getMaster()),
                appId,Long.parseLong(elasticSearchConfig.getCheckInterval()));

        log.info("End time : {}", dateFormat.format(new Date()));
    }


}
