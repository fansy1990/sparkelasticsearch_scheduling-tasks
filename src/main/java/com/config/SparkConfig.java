package com.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: fansy
 * @Time: 2018/1/18 14:14
 * @Email: fansy1990@foxmail.com
 */
@Configuration
public class SparkConfig {
    public String getAppResource() {
        return appResource;
    }

    public void setAppResource(String appResource) {
        this.appResource = appResource;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    @Value("${elasticsearch2hive.job.mainClass}")
    private String mainClass;
    @Value("${spark.appResource}")
    private String appResource;
    @Value("${spark.master}")
    private String master;
    @Value("${spark.executor.memory}")
    private String executorMemory;
    @Value("${spark.cores.max}")
    private String coresMax;
    @Value("${spark.executor.cores}")
    private String executorCores;
    @Value("${spark.executor.extraClassPath}")
    private String extraClassPath;
    @Value("${spark.submit.deployMode}")
    private String deployMode;
    @Value("${spark.driver.supervise}")
    private String supervise;

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getCoresMax() {
        return coresMax;
    }

    public void setCoresMax(String coresMax) {
        this.coresMax = coresMax;
    }

    public String getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(String executorCores) {
        this.executorCores = executorCores;
    }

    public String getExtraClassPath() {
        return extraClassPath;
    }

    public void setExtraClassPath(String extraClassPath) {
        this.extraClassPath = extraClassPath;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getSupervise() {
        return supervise;
    }

    public void setSupervise(String supervise) {
        this.supervise = supervise;
    }
}
