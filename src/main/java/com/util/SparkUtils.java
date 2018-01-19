package com.util;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.RestSubmissionClient;

/**
 * @Author: fansy
 * @Time: 2018/1/18 16:35
 * @Email: fansy1990@foxmail.com
 */
public class SparkUtils {

    public static SparkConf getSparkConf(String master,String executorCores ,String deployMode
        ,String appResource,String executorMemory ,String coresMax,String supervise,String extraClassPath) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(master);
        sparkConf.setAppName("spark engine "+ System.currentTimeMillis());
        sparkConf.set("spark.executor.cores",executorCores);
        sparkConf.set("spark.submit.deployMode",deployMode);
        sparkConf.set("spark.jars",appResource);
        sparkConf.set("spark.executor.memory",executorMemory);
        sparkConf.set("spark.cores.max",coresMax);
        sparkConf.set("spark.driver.supervise",supervise);
        sparkConf.set("spark.executor.extraClassPath", extraClassPath);
        sparkConf.set("spark.driver.extraClassPath", extraClassPath);
        return sparkConf ;
    }
    private static RestSubmissionClient client;
    public static RestSubmissionClient getClient(String master) {
        if(client == null){
            client = new RestSubmissionClient(master);
        }
        return client;
    }
}
