package org.apache.spark.deploy.rest;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Spark 引擎：
 * 1）调用Spark算法,提交任务到Spark StandAlone集群，并返回id；
 * 2）根据id监控Spark 任务；
 * Created by fansy on 2017/11/16.
 */
@Component
public class SparkEngine2 {
    private static final Logger log = LoggerFactory.getLogger(SparkEngine2.class);

//    private static final String MASTER="spark://server2.tipdm.com:6066";
//    private static final String APPNAME="wordcount 2";

    public  String submit(SparkConf sparkConf,String appResource,String mainClass,String ...args){
        Map<String,String> env = filterSystemEnvironment(System.getenv());
        CreateSubmissionResponse response = null;
        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(appResource, mainClass, args, sparkConf, toScalaMap(env));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return response.submissionId();
    }

    private static RestSubmissionClient client = null;
    public  void monitory(RestSubmissionClient client ,String appId,Long time){

        SubmissionStatusResponse response = null;
        boolean finished =false;
        int i =0;
        while(!finished) {
            try {
                response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
                log.info("i:{}\t DriverState :{}",i++,response.driverState());
                if("FINISHED" .equals(response.driverState()) || "ERROR".equals(response.driverState())
                        || "FAILED".equals(response.driverState())){
                    finished = true;
                    log.warn("appId:{}, final state:",new Object[]{appId,response.driverState()});
                }
                Thread.sleep(time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("Monitor done!");
    }

    private static Map<String, String> filterSystemEnvironment(Map<String, String> env) {
        Map<String,String> map = new HashMap<>();
        for(Map.Entry<String,String> kv : env.entrySet()){
            if(kv.getKey().startsWith("SPARK_") && kv.getKey() != "SPARK_ENV_LOADED"
                    || kv.getKey().startsWith("MESOS_")){
                map.put(kv.getKey(),kv.getValue());
            }
        }
        return map;
    }

    /**
     * 这个方法实际返回的结果也是空，所以直接返回空即可
     * @param m
     * @param <A>
     * @param <B>
     * @return
     */
    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
//        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
//                Predef.<Tuple2<A, B>>conforms()
//        )
        return new  scala.collection.immutable.HashMap();
    }
}