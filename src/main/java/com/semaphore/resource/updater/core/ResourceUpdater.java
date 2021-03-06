package com.semaphore.resource.updater.core;

import com.semaphore.resource.updater.db.DbAccessor;
import com.semaphore.resource.updater.exceptions.*;
import com.semaphore.resource.updater.cache.CacheAccessor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 普通资源更新
 * 不支持预占
 * @date 2021/10/12 2:25 下午
 */
@Slf4j
public class ResourceUpdater {
    /**
     * 数据库操作
     */
    protected DbAccessor dbAccessor;

    /**
     * 构造
     * @param dataSource
     * @param transactionTemplate
     * @param redissonClient
     * @param dbResourceTableName
     * @param dbResourceKeyColumnName
     * @param dbResourceAvailablePermitColumnName
     * @param dbResourcePreLockPermitColumnName
     */
    public ResourceUpdater(DataSource dataSource,
                           TransactionTemplate transactionTemplate,
                           RedissonClient redissonClient,
                           String dbResourceTableName,
                           String dbResourceKeyColumnName,
                           String dbResourceAvailablePermitColumnName,
                           String dbResourcePreLockPermitColumnName){
        if(Objects.isNull(dataSource)){
            throw new ResourceRunException("dataSource不能为空");
        }
        if(Objects.isNull(transactionTemplate)){
            throw new ResourceRunException("transactionTemplate不能为空");
        }
        if(Objects.isNull(redissonClient)){
            throw new ResourceRunException("redissonClient不能为空");
        }
        if(Objects.isNull(dbResourceTableName)){
            throw new ResourceRunException("dbResourceTableName不能为空");
        }
        if(Objects.isNull(dbResourceKeyColumnName)){
            throw new ResourceRunException("dbResourceKeyColumnName不能为空");
        }
        if(Objects.isNull(dbResourceAvailablePermitColumnName)){
            throw new ResourceRunException("dbResourceAvailablePermitColumnName不能为空");
        }
        if(Objects.isNull(dbResourcePreLockPermitColumnName)){
            throw new ResourceRunException("dbResourcePreLockPermitColumnName不能为空");
        }
        CacheAccessor.redissonClient = redissonClient;
        dbAccessor = new DbAccessor(dataSource,transactionTemplate,dbResourceTableName,dbResourceKeyColumnName,dbResourceAvailablePermitColumnName,dbResourcePreLockPermitColumnName);
    }

    public void setAutoAdjustRate(int rate){
        CacheAccessor.setAutoAdjustRate(rate);
    }

    /**
     * 查询可用资源数量
     * @param queryResourceParam
     * @return
     */
    public QueryResourceResult queryAvailable(QueryResourceParam queryResourceParam)
            throws LockWaitException, InterruptedException {
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(1);
        updateResourceParamList.add(UpdateResourceParam.builder().resourceId(queryResourceParam.getResourceId()).build());
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResourceAndInit(updateResourceParamList);

        return CacheAccessor.queryAvailable(queryResourceParam);
    }

    /**
     * 批量查询可用资源数量
     * @param queryResourceParamList
     * @return
     */
    public List<QueryResourceResult> queryAvailable(List<QueryResourceParam> queryResourceParamList)
            throws LockWaitException, InterruptedException {
        List<UpdateResourceParam> updateResourceParamList = queryResourceParamList
                .stream()
                .map(queryResourceParam -> UpdateResourceParam.builder().resourceId(queryResourceParam.getResourceId()).build())
                .collect(Collectors.toList());
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResourceAndInit(updateResourceParamList);

        List<QueryResourceResult> resultList = new ArrayList<>();
        for(QueryResourceParam queryResourceParam : queryResourceParamList){
            QueryResourceResult queryResourceResult = CacheAccessor.queryAvailable(queryResourceParam);
            resultList.add(queryResourceResult);
        }
        return resultList;
    }

    /**
     * 尝试扣减资源 直接扣减可用资源 不支持预占
     * @param updateResourceParamSet
     * @throws ResourceUpdateException
     * @throws ResourceWaitException
     */
    public void trySubtractAvailable(Set<UpdateResourceParam> updateResourceParamSet)
            throws ResourceUpdateException, LockWaitException, InterruptedException, DataUnConsistentException {
        if(Objects.isNull(updateResourceParamSet) || updateResourceParamSet.size() == 0){
            throw new ResourceRunException("trySubtractResource参数为空");
        }
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResourceAndInit(updateResourceParamList);

        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();

        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        //加读锁
        ReadWriteLock.availableTryLockRead(resourceIdList);

        //更新
        try {
            CacheAccessor.tryAcquireAvailableSemaphore(updateResourceParamList,dbAccessor);
            dbAccessor.subtractAvailableResource(updateResourceParamList);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("更新资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("更新资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 尝试扣减资源 直接扣减可用资源 不支持预占
     * @param updateResourceParam
     * @throws ResourceUpdateException
     * @throws ResourceWaitException
     */
    public void trySubtractOneAvailable(UpdateResourceParam updateResourceParam)
            throws ResourceUpdateException, LockWaitException, InterruptedException, DataUnConsistentException {
        if(Objects.isNull(updateResourceParam)){
            throw new ResourceRunException("trySubtractOneResource参数为空");
        }

        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        updateResourceParamList.add(updateResourceParam);
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResourceAndInit(updateResourceParamList);

        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();

        //加读锁
        String resourceId = updateResourceParam.getResourceId();
        ReadWriteLock.availableTryLockRead(resourceId);

        //更新
        try {
            CacheAccessor.tryAcquireAvailableSemaphore(updateResourceParam,dbAccessor);
            dbAccessor.subtractOneAvailableResource(updateResourceParam);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("更新可用资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("更新可用资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 批量增加可用资源信号量（数据库和缓存同时增加）
     * @param updateResourceParamSet
     */
    public void addAvailable(Set<UpdateResourceParam> updateResourceParamSet)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        if(Objects.isNull(updateResourceParamSet) || updateResourceParamSet.size() == 0){
            return;
        }
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();
        //加读锁
        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        ReadWriteLock.availableTryLockRead(resourceIdList);
        //增加
        try {
            dbAccessor.addAvailableResource(updateResourceParamList);
            CacheAccessor.tryLeaseAvailableSemaphore(updateResourceParamList,dbAccessor);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("增加可用资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("增加可用资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 单个增加可用资源信号量（数据库和缓存同时增加）
     * @param updateResourceParam
     * @throws LockWaitException
     * @throws InterruptedException
     * @throws DataUnConsistentException
     * @throws ResourceUpdateException
     */
    public void addOneAvailable(UpdateResourceParam updateResourceParam)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        String resourceId = updateResourceParam.getResourceId();
        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        updateResourceParamList.add(updateResourceParam);
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //加读锁
        ReadWriteLock.availableTryLockRead(resourceId);
        //增加
        try {
            dbAccessor.addAvailableResource(updateResourceParamList);
            CacheAccessor.tryLeaseAvailableSemaphore(updateResourceParamList,dbAccessor);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("增加可用资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("增加可用资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 删除可用资源信号量缓存
     * @param resourceKey
     */
    public void deleteAvailableCache(String resourceKey) throws LockWaitException, InterruptedException {
        CacheAccessor.deleteAvailableResourceSemaphore(resourceKey);
    }

    //=============
    //=============
    //=============

    protected List<ResourcePermit> getResourcePermitsFromDb(List<String> resourceIdList){
        if(Objects.isNull(resourceIdList)){
            return null;
        }
        return dbAccessor.queryResource(resourceIdList);
    }

    protected ResourcePermit getOneResourcePermitFromDb(String resourceId){
        if(Objects.isNull(resourceId)){
            return null;
        }
        return dbAccessor.queryOneResource(resourceId);
    }

    protected void registerDbTransactionRollBackHock() {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCompletion(int status) {
                if(TransactionSynchronization.STATUS_ROLLED_BACK == status){
                    //数据库回滚了 将缓存中的信号量也回滚
                    CacheAccessor.leaseAcquiredAvailableSemaphore();
                    CacheAccessor.acquireLeasedAvailableSemaphore();
                }
                ReadWriteLock.leaseHoldAvailableReadLock();
                ReadWriteLock.leaseHoldAvailableWriteLock();
                //清除所有ThreadLocal
                clearAllThreadLocal();
            }
        });
    }

    protected void clearAllThreadLocal(){
        CacheAccessor.clearThreadLocal();
        ReadWriteLock.clearThreadLocal();
    }

    /**
     * 检查是否存在不一致地可用库存信号量，如果存在，尝试进行初始化
     * @param updateResourceParamList
     * @throws LockWaitException
     * @throws InterruptedException
     */
    protected void checkNotConsistenceAvailableResourceAndInit(List<UpdateResourceParam> updateResourceParamList)
            throws LockWaitException, InterruptedException {
        List<String> notConsistenceKeys = CacheAccessor.checkAvailableResourceConsistence(updateResourceParamList);
        if(Objects.nonNull(notConsistenceKeys)){
            //存在缓存数据库不一致的key
            //尝试加写锁进行初始化
            CacheAccessor.initAvailableSemaphorePermit(notConsistenceKeys,dbAccessor);
            ReadWriteLock.leaseHoldAvailableWriteLock();
            StringBuilder sb = new StringBuilder();
            notConsistenceKeys.forEach(key -> sb.append(key + ","));
            log.info("可用资源缓存与数据库存在不一致:" + sb + " 已进行重新初始化");
        }
    }

    /**
     * 检查是否存在不一致地预占库存信号量，如果存在，尝试进行初始化
     * @param updateResourceParamList
     * @throws LockWaitException
     * @throws InterruptedException
     */
    protected void checkNotConsistencePreLockedResourceAndInit(List<UpdateResourceParam> updateResourceParamList)
            throws LockWaitException, InterruptedException {
        List<String> notConsistenceKeys = CacheAccessor.checkPreLockedResourceConsistence(updateResourceParamList);
        if(Objects.nonNull(notConsistenceKeys)){
            //存在不一致的key
            //尝试加写锁进行初始化
            CacheAccessor.initPreLockedSemaphorePermit(notConsistenceKeys,dbAccessor);
            ReadWriteLock.leaseHoldPreLockedWriteLock();
            StringBuilder sb = new StringBuilder();
            notConsistenceKeys.forEach(key -> sb.append(key + ","));
            log.info("预占资源缓存与数据库存在不一致:" + sb + " 已进行重新初始化");
        }
    }
}
