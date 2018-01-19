package com.jobs;

import com.config.ElasticSearchConfig;
import com.config.SparkConfig;
import com.util.SparkUtils;
import org.apache.spark.deploy.rest.SparkEngine2;
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
public class MultiElasticsearch2HiveJob {
    private static final Logger log = LoggerFactory.getLogger(MultiElasticsearch2HiveJob.class);
    @Autowired
    private SparkConfig sparkConfig;
    @Autowired
    private ElasticSearchConfig elasticSearchConfig;

    @Autowired
    private SparkEngine2 sparkEngine;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
    @Scheduled(cron = "${multielasticsearch2hive.job.cron}")
    public void synEs2Hive() {
        log.info("Start time : {}", dateFormat.format(new Date()));

        // 一共 256 *4 G内存， 48*4核
        // 60G , 40 核/10核 = 4 instances
        // 16G , 40 核/4核 = 10 instances
        // 8G , 80 核/4核 = 20 instances
        String[] executorCores = {"10", "4", "4"};

        String[] executorMemorys = {"60g", "16g", "8g"};

        String[] coresMax = {"40", "40", "80"};

        for (int i = 0; i < executorCores.length; i++) {
            long start = System.currentTimeMillis();
            startAndMonitor(executorCores[i], executorMemorys[i], coresMax[i]);
            long end = System.currentTimeMillis();
            log.info("executor memory:{}, executor core: {}, executor instances:{}, time : {}",new Object[]{
                    executorMemorys[i],executorCores[i],
                    Integer.parseInt(coresMax[i]) / Integer.parseInt(executorCores[i]),
                    (end-start) *1.0/1000 / 60 +" minutes !"
            });
        }
    }
    private void startAndMonitor(String executorCore, String executorMemory, String maxCore) {
        String appId = sparkEngine.submit(
                SparkUtils.getSparkConf(sparkConfig.getMaster(),executorCore,
                        sparkConfig.getDeployMode(),sparkConfig.getAppResource(),
                        executorMemory,maxCore,
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

        sparkEngine.monitory(SparkUtils.getClient(sparkConfig.getMaster()),
                appId,Long.parseLong(elasticSearchConfig.getCheckInterval()));
    }


}
