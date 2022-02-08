package com.semaphore.resource.updater.config;

import com.semaphore.resource.updater.TestHighResourceUpdater;
import com.semaphore.resource.updater.core.HighResourceUpdater;
import com.semaphore.resource.updater.core.ResourceUpdater;
import com.semaphore.resource.updater.db.DbAccessor;
import com.zaxxer.hikari.HikariDataSource;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;

/**
 *
 * @date 2022/1/25 2:24 PM
 */
@Configuration
public class SpringBootConfig {
    private static final String DB_USER_NAME = "root";
    private static final String DB_PWD = "root123";
    private static final String JDBC_URL = "jdbc:mysql://192.168.0.31:3306/resource_test?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT%2B8&tinyInt1isBit=false";

    private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String TABLE_NAME = "t_resource_test";
    private static final String TABLE_RESOURCE_COLUMN_NAME = "name";
    private static final String TABLE_RESOURCE_AVAILABLE_COLUMN_NAME = "available_num";
    private static final String TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME = "pre_lock_num";

    @Bean
    public HikariDataSource hikariDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setUsername(DB_USER_NAME);
        hikariDataSource.setPassword(DB_PWD);
        hikariDataSource.setJdbcUrl(JDBC_URL);
        hikariDataSource.setDriverClassName(MYSQL_DRIVER_CLASS_NAME);
        hikariDataSource.setMaxLifetime(3000);
        hikariDataSource.setMaximumPoolSize(100);
        hikariDataSource.setPoolName("Resource-Updater");
        return hikariDataSource;
    }

    @Bean
    public DataSourceTransactionManager transactionManager(HikariDataSource hikariDataSource){
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(hikariDataSource);
        return transactionManager;
    }

    @Bean
    public TransactionTemplate transactionTemplate(DataSourceTransactionManager dataSourceTransactionManager){
        TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
        return transactionTemplate;
    }

    @Bean
    public RedissonClient redissonClient(){
        Config config = null;
        try {
            config = Config.fromYAML(TestHighResourceUpdater.class.getClassLoader().getResource("redisson-config.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        RedissonClient redissonClient = Redisson.create(config);
        return redissonClient;
    }

    @Bean
    public ResourceUpdater commonResourceUpdater(HikariDataSource hikariDataSource, TransactionTemplate transactionTemplate, RedissonClient redissonClient){
        ResourceUpdater resourceUpdater = new ResourceUpdater(hikariDataSource,transactionTemplate,redissonClient,TABLE_NAME
                ,TABLE_RESOURCE_COLUMN_NAME
                ,TABLE_RESOURCE_AVAILABLE_COLUMN_NAME
                ,TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME);
        return resourceUpdater;
    }

    @Bean
    public HighResourceUpdater highResourceUpdater(HikariDataSource hikariDataSource, TransactionTemplate transactionTemplate, RedissonClient redissonClient){
        HighResourceUpdater highResourceUpdater = new HighResourceUpdater(hikariDataSource,transactionTemplate,redissonClient,TABLE_NAME
                ,TABLE_RESOURCE_COLUMN_NAME
                ,TABLE_RESOURCE_AVAILABLE_COLUMN_NAME
                ,TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME);
        return highResourceUpdater;
    }

    @Bean
    public DbAccessor dbAccessor(HikariDataSource dataSource, TransactionTemplate transactionTemplate){
        DbAccessor dbAccessor = new DbAccessor(dataSource,transactionTemplate,TABLE_NAME,TABLE_RESOURCE_COLUMN_NAME,TABLE_RESOURCE_AVAILABLE_COLUMN_NAME,TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME);
        return dbAccessor;
    }
}
