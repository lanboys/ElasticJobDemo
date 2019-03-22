package com.bing.lan.elastic.demo.configuration;

import com.bing.lan.elastic.demo.job.SimpleJobDemo;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticJobConfig {

    /**
     * 配置zk注册中心
     */
    @Bean(initMethod = "init")
    public ZookeeperRegistryCenter regCenter(
            @Value("${regCenter.serverList}") final String serverList,
            @Value("${regCenter.namespace}") final String namespace) {
        return new ZookeeperRegistryCenter(new ZookeeperConfiguration(serverList, namespace));
    }

    /**
     * 配置任务监听器
     */
    @Bean
    public ElasticJobListener elasticJobListener() {
        return new MyElasticJobListener();
    }

    /**
     * 配置任务详细信息
     */
    private LiteJobConfiguration getLiteJobConfiguration(final Class<? extends SimpleJob> jobClass,
            final String cron,
            final int shardingTotalCount,
            final String shardingItemParameters) {

        JobCoreConfiguration build = JobCoreConfiguration.newBuilder(jobClass.getName(), cron, shardingTotalCount)
                .shardingItemParameters(shardingItemParameters).build();

        SimpleJobConfiguration jobConfig = new SimpleJobConfiguration(build
                , jobClass.getCanonicalName());

        return LiteJobConfiguration.newBuilder(jobConfig)
                .overwrite(true)
                .build();
    }

    @Bean(initMethod = "init")
    public JobScheduler simpleJobScheduler(
            ZookeeperRegistryCenter regCenter,
            ElasticJobListener elasticJobListener,
            final SimpleJobDemo simpleJob,
            @Value("${stockJob.cron}") final String cron,
            @Value("${stockJob.shardingTotalCount}") final int shardingTotalCount,
            @Value("${stockJob.shardingItemParameters}") final String shardingItemParameters) {

        LiteJobConfiguration config = getLiteJobConfiguration(simpleJob.getClass(),
                cron, shardingTotalCount, shardingItemParameters);

        return new SpringJobScheduler(simpleJob, regCenter, config, elasticJobListener);
    }
}
