package com.bing.lan.elastic.demo.configuration;

import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * 任务总监听器
 */
public class MyElasticJobListener implements ElasticJobListener {

    private static final Logger logger = LoggerFactory.getLogger(MyElasticJobListener.class);
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
    private long beginTime = 0;

    @Override
    public void beforeJobExecuted(ShardingContexts shardingContexts) {
        beginTime = System.currentTimeMillis();
        logger.info("===>{} job begin time: {} <===", shardingContexts.getJobName(), simpleDateFormat.format(beginTime));
    }

    @Override
    public void afterJobExecuted(ShardingContexts shardingContexts) {
        long endTime = System.currentTimeMillis();
        logger.info("===>{} job end time: {},total cast: {} <===", shardingContexts.getJobName(),
                simpleDateFormat.format(endTime), endTime - beginTime);
    }
}
