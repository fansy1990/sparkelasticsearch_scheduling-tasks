package com.jobs;

import com.config.ElasticSearchConfig;
import com.config.SparkConfig;
import org.apache.spark.api.java.JavaSparkContext;
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

import org.apache.spark.SparkConf;

/**
 * 同步Elasticsearch 数据到Hive任务
 * 1. 直接发送任务到spark集群是可以的，但是如果集群连接不了启动Driver的节点（也就是运行这个class的节点），
 * 那么任务会失败，后台提示资源不足，master使用7077
 * 2. spark环境中如果没有Elasticsearch相关jar包，还是会报jar包找不到；
 * Created by fansy on 2018/1/18.
 */
@Component
public class ElasticsearchJob {
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchJob.class);
    @Autowired
    private SparkConfig sparkConfig;
    @Autowired
    private ElasticSearchConfig elasticSearchConfig;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    @Scheduled(cron = "${elasticsearch.job.cron}")
    public void synEs2Hive() {
        log.info("The time is now {}", dateFormat.format(new Date()));
        SparkConf sparkConf = new SparkConf().setAppName("simple_test");
        sparkConf.set("spark.master", sparkConfig.getMaster());
        sparkConf.set("spark.executor.memory",sparkConfig.getExecutorMemory());
        sparkConf.set("spark.cores.max",sparkConfig.getCoresMax());
        sparkConf.set("spark.executor.cores",sparkConfig.getExecutorCores());
        sparkConf.set("spark.executor.extraClassPath", sparkConfig.getExtraClassPath());
        sparkConf.set("spark.driver.extraClassPath", sparkConfig.getExtraClassPath());
        // the code below should not be change
        sparkConf.set("spark.submit.deployMode",sparkConfig.getDeployMode());
        sparkConf.set("spark.driver.supervise",sparkConfig.getSupervise());

//        conf.set("es.read.field.exclude", "time_tags, vod_cat_tags");
        sparkConf.set("es.nodes", elasticSearchConfig.getNodes());
        sparkConf.set("es.port", elasticSearchConfig.getPort());
        sparkConf.set("es.mapping.date.rich", elasticSearchConfig.getDataRich());

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sql = new SQLContext(sc);
        DataFrame resource = JavaEsSparkSQL.esDF(sql, elasticSearchConfig.getResource());

        log.info("table size :"+ resource.count());
        sc.stop();
        log.info("The time is now {}", dateFormat.format(new Date()));
    }


}
