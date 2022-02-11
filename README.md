# 概要说明
本组件支持高吞吐的资源更新需求，是通过**信号量机制**结合**读写锁**来实现的。<br>
信号量机制用于支持高吞吐的资源写，读写锁用于保证对缓存信号量的安全初始化。
典型的应用场景是并发场景下对库存的更新操作（例如秒杀场景）。 <br>
其中信号量机制与读写锁复用redisson的实现。

# 前置条件
本组件强制依赖 **spring-jdbc** **spring-tx** **redisson**

# 设计原则
## 数据
所有查询都通过读取缓存实现，更新操作会同时更新缓存和数据库。

## 一致性
数据库与缓存不支持强一致，但是可以做到最终一致。<br>
一致的最小粒度是方法，例如resourceUpdater.trySubtractAvailable(updateResourceParams)执行过程中会出现缓存和数据库的不一致，当方法结束之后两者会保持一致。<br>
这是因为横跨两个系统，很难做到强一致。<br>
这样就会出现查询到库存够但是实际不够或查询到库存不够但实际够的情况，因为组件的方法是最终一致的， 所以在这些情况下，组件方法内部可以保证数据更新不会出错。

## 自调节
自调节的粒度是单个资源<br>
当出现以下两种情形时，组件会进行自调节：<br>
- 更新资源时，数据库没有满足数量要求的资源。例如要求3个，数据库只有1个。
- 查询资源时，缓存资源数不满足要求。

调节方法：<br>
- 标记资源为不一致状态。等待下次重新初始化。
- 根据配置的阈值（resource_auto_adjust_rate），判定是否需要调节，如果满足条件，标记资源为不一致状态，等待下次重新初始化。

resource_auto_adjust_rate可以设置为 0-10 之间的整数 设置为0 表示永远不调节；设置为1 表示有10%的概率调节；设置为10 表示一定调节。<br>
通过setAutoAdjustRate方法设置。如果不设置，默认值为3。


# 使用方法
## 导入依赖
```xml
<dependency>
    <groupId>io.github.fashionsuperman</groupId>
    <artifactId>resource-updater</artifactId>
    <version>1.1</version>
</dependency>
```
## 配置组件
```java
public class SpringBootConfig {
    private static final String DB_USER_NAME = "root";
    private static final String DB_PWD = "root123";
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/resource_test?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT%2B8&tinyInt1isBit=false";

    private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    /**
     * 资源对应的数据库表名（例如商品库存表）
     */
    private static final String TABLE_NAME = "t_resource_test";
    /**
     * 唯一标记资源的表列名（例如商品编码）
     */
    private static final String TABLE_RESOURCE_COLUMN_NAME = "name";
    /**
     * 可用资源列名（例如可用库存数对应的列名）
     */
    private static final String TABLE_RESOURCE_AVAILABLE_COLUMN_NAME = "available_num";
    /**
     * 预占资源列表（例如预占库存数对应的列名，如果不需要预占库存，此字段可设置为和可用资源列名相同）
     */
    private static final String TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME = "pre_lock_num";

    /**
     * 配置数据源
     * @return
     */
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

    /**
     * 配置事务管理器
     * @param hikariDataSource
     * @return
     */
    @Bean
    public DataSourceTransactionManager transactionManager(HikariDataSource hikariDataSource){
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(hikariDataSource);
        return transactionManager;
    }

    /**
     * 配置事务模板
     * @param dataSourceTransactionManager
     * @return
     */
    @Bean
    public TransactionTemplate transactionTemplate(DataSourceTransactionManager dataSourceTransactionManager){
        TransactionTemplate transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
        return transactionTemplate;
    }

    /**
     * 配置redisson
     * @return
     */
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

    /**
     * 配置资源更新器（不支持预占）
     * @param hikariDataSource
     * @param transactionTemplate
     * @param redissonClient
     * @return
     */
    @Bean
    public ResourceUpdater commonResourceUpdater(HikariDataSource hikariDataSource, TransactionTemplate transactionTemplate, RedissonClient redissonClient){
        ResourceUpdater resourceUpdater = new ResourceUpdater(hikariDataSource,transactionTemplate,redissonClient,TABLE_NAME
                ,TABLE_RESOURCE_COLUMN_NAME
                ,TABLE_RESOURCE_AVAILABLE_COLUMN_NAME
                ,TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME);
        return resourceUpdater;
    }

    /**
     * 配置资源更新器（支持预占）
     * @param hikariDataSource
     * @param transactionTemplate
     * @param redissonClient
     * @return
     */
    @Bean
    public HighResourceUpdater highResourceUpdater(HikariDataSource hikariDataSource, TransactionTemplate transactionTemplate, RedissonClient redissonClient){
        HighResourceUpdater highResourceUpdater = new HighResourceUpdater(hikariDataSource,transactionTemplate,redissonClient,TABLE_NAME
                ,TABLE_RESOURCE_COLUMN_NAME
                ,TABLE_RESOURCE_AVAILABLE_COLUMN_NAME
                ,TABLE_RESOURCE_PRE_LOCK_COLUMN_NAME);
        return highResourceUpdater;
    }
}
```
**注意**：redisson本身还需要指定连接信息，需要在项目资源路径下提供redisson-config.yml文件
## 业务使用
```java
@Service
public class MockBizService {
    @Resource
    @Qualifier("commonResourceUpdater")
    private ResourceUpdater resourceUpdater;
    @Resource
    private HighResourceUpdater highResourceUpdater;

    /**
     * 信号量方式 批量扣减库存
     * @param updateResourceParams
     */
    @Transactional(rollbackFor = Exception.class)
    public void trySubtractResource(HashSet<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        //库存查询 库存判断
        resourceUpdater.queryAvailable();
        //生成订单 记录流水 
        //扣减库存
        resourceUpdater.trySubtractAvailable(updateResourceParams);
    }

    /**
     * 信号量方式 批量扣减可用库存并增加预占库存
     * @param updateResourceParams
     */
    @Transactional(rollbackFor = Exception.class)
    public void trySubtractAvailableAddPreLock(Set<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        //库存查询 库存判断
        highResourceUpdater.queryAvailable();
        //生成订单 记录流水 
        //扣减库存
        highResourceUpdater.trySubtractAvailableAddPreLock(updateResourceParams);
    }
}
```
**注意** 业务方法必须开启事务

