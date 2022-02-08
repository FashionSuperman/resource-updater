package com.semaphore.resource.updater.core;

import com.semaphore.resource.updater.exceptions.DataUnConsistentException;
import com.semaphore.resource.updater.exceptions.LockWaitException;
import com.semaphore.resource.updater.exceptions.ResourceRunException;
import com.semaphore.resource.updater.exceptions.ResourceUpdateException;
import com.semaphore.resource.updater.cache.CacheAccessor;
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
 * 支持预占的资源更新
 *
 * @date 2022/1/21 2:11 PM
 */
public class HighResourceUpdater extends ResourceUpdater{
    /**
     * 构造
     *
     * @param dataSource
     * @param transactionTemplate
     * @param redissonClient
     * @param dbResourceTableName
     * @param dbResourceKeyColumnName
     * @param dbResourceAvailablePermitColumnName
     * @param dbResourcePreLockPermitColumnName
     */
    public HighResourceUpdater(DataSource dataSource,
                               TransactionTemplate transactionTemplate,
                               RedissonClient redissonClient,
                               String dbResourceTableName,
                               String dbResourceKeyColumnName,
                               String dbResourceAvailablePermitColumnName,
                               String dbResourcePreLockPermitColumnName) {
        super(dataSource, transactionTemplate, redissonClient, dbResourceTableName, dbResourceKeyColumnName, dbResourceAvailablePermitColumnName, dbResourcePreLockPermitColumnName);
    }

    /**
     * 查询预占资源数量
     * @param resourceId
     * @return
     */
    public Integer queryPreLocked(String resourceId){
        return CacheAccessor.queryPreLocked(resourceId);
    }

    /**
     * 批量查询预占资源数量
     * @param resourceIdList
     * @return
     */
    public Map<String,Integer> queryPreLocked(List<String> resourceIdList){
        Map<String,Integer> resultMap = new HashMap<>();
        for(String resourceId : resourceIdList){
            Integer num = queryPreLocked(resourceId);
            resultMap.put(resourceId,num);
        }
        return resultMap;
    }

    /**
     * 扣减可用 增加预占
     * @param updateResourceParamSet
     * @throws ResourceUpdateException
     * @throws LockWaitException
     * @throws InterruptedException
     * @throws DataUnConsistentException
     */
    public void trySubtractAvailableAddPreLock(Set<UpdateResourceParam> updateResourceParamSet)
            throws ResourceUpdateException, LockWaitException, InterruptedException, DataUnConsistentException {
        if(Objects.isNull(updateResourceParamSet) || updateResourceParamSet.size() == 0){
            throw new ResourceRunException("trySubtractAvailableAddPreLock参数为空");
        }
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查预占资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkPreLockedSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResource(updateResourceParamList);
        //检查是否存在不一致地预占资源Key
        checkNotConsistencePreLockedResource(updateResourceParamList);

        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();

        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        //加读锁
        ReadWriteLock.availableTryLockRead(resourceIdList);
        ReadWriteLock.preLockedTryLockRead(resourceIdList);

        //更新
        try {
            CacheAccessor.tryAcquireAvailableSemaphore(updateResourceParamList,dbAccessor);
            CacheAccessor.tryLeasePreLockedSemaphore(updateResourceParamList,dbAccessor);
            dbAccessor.subtractAvailableAndAddPreLockResource(updateResourceParamList);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("更新资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("更新资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 释放(扣减)预占 增加可用
     * @see HighResourceUpdater#trySubtractAvailableAddPreLock(Set)  的逆向
     * @param updateResourceParamSet
     * @throws ResourceUpdateException
     * @throws LockWaitException
     * @throws InterruptedException
     * @throws DataUnConsistentException
     */
    public void trySubtractPreLockAddAvailable(Set<UpdateResourceParam> updateResourceParamSet)
            throws ResourceUpdateException, LockWaitException, InterruptedException, DataUnConsistentException {
        if(Objects.isNull(updateResourceParamSet) || updateResourceParamSet.size() == 0){
            throw new ResourceRunException("trySubtractPreLockAddAvailable参数为空");
        }
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //检查可用资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkAvailableSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查预占资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkPreLockedSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地可用资源Key
        checkNotConsistenceAvailableResource(updateResourceParamList);
        //检查是否存在不一致地预占资源Key
        checkNotConsistencePreLockedResource(updateResourceParamList);

        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();

        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        //加读锁
        ReadWriteLock.availableTryLockRead(resourceIdList);
        ReadWriteLock.preLockedTryLockRead(resourceIdList);

        //更新
        try {
            //因为如果先操作缓存信号量会导致可用数量缓存判断满足条件，但是数据库判断不满足的情况，且该操作低频，固先操作数据库
            dbAccessor.subtractPreLockedAndAddAvailableResource(updateResourceParamList);
            CacheAccessor.tryAcquirePreLockedSemaphore(updateResourceParamList,dbAccessor);
            CacheAccessor.tryLeaseAvailableSemaphore(updateResourceParamList,dbAccessor);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("更新资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("更新资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 扣减预占
     * @param updateResourceParamSet
     */
    public void trySubtractPreLock(Set<UpdateResourceParam> updateResourceParamSet)
            throws ResourceUpdateException, LockWaitException, InterruptedException, DataUnConsistentException {
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //检查预占资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkPreLockedSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //检查是否存在不一致地预占资源Key
        checkNotConsistencePreLockedResource(updateResourceParamList);
        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();
        //加读锁
        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        ReadWriteLock.preLockedTryLockRead(resourceIdList);
        //更新
        try {
            CacheAccessor.tryAcquirePreLockedSemaphore(updateResourceParamList,dbAccessor);
            dbAccessor.subtractPreLockedResource(updateResourceParamList);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("更新资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("更新资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 增加预占资源信号量（数据库和缓存同时增加）
     * @param updateResourceParamSet
     * @throws LockWaitException
     * @throws InterruptedException
     * @throws DataUnConsistentException
     * @throws ResourceUpdateException
     */
    public void addPreLock(Set<UpdateResourceParam> updateResourceParamSet)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        if(Objects.isNull(updateResourceParamSet) || updateResourceParamSet.size() == 0){
            return;
        }
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>(updateResourceParamSet);
        //防止死锁，resourceId排序
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        //注册事务回滚之后钩子
        registerDbTransactionRollBackHock();
        //检查预占资源信号量是否初始化 否则进行初始化
        CacheAccessor.checkPreLockedSemaphoreInitializedOrInit(updateResourceParamList,dbAccessor);
        //加读锁
        List<String> resourceIdList = updateResourceParamList.stream().map(param -> param.getResourceId()).collect(Collectors.toList());
        ReadWriteLock.preLockedTryLockRead(resourceIdList);
        //增加
        try {
            dbAccessor.addPreLockResource(updateResourceParamList);
            CacheAccessor.tryLeasePreLockedSemaphore(updateResourceParamList,dbAccessor);
        }catch (UndeclaredThrowableException e){
            throw new DataUnConsistentException("增加预占资源数量失败,errMsg:" + e.getUndeclaredThrowable().getMessage());
        }catch (Throwable e){
            throw new ResourceUpdateException("增加预占资源数量失败,errMsg:" + e.getMessage());
        }
    }

    /**
     * 删除预占资源信号量缓存
     * @param resourceKey
     */
    public void deletePreLockCache(String resourceKey) throws LockWaitException, InterruptedException {
        CacheAccessor.deletePreLockedResourceSemaphore(resourceKey);
    }

    @Override
    protected void registerDbTransactionRollBackHock() {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCompletion(int status) {
                if(TransactionSynchronization.STATUS_ROLLED_BACK == status){
                    //数据库回滚了 将缓存中的信号量也回滚
                    CacheAccessor.leaseAcquiredAvailableSemaphore();
                    CacheAccessor.acquireLeasedAvailableSemaphore();
                    CacheAccessor.leaseAcquiredPreLockedSemaphore();
                    CacheAccessor.acquireLeasedPreLockedSemaphore();
                }
                ReadWriteLock.leaseHoldAvailableReadLock();
                ReadWriteLock.leaseHoldAvailableWriteLock();
                ReadWriteLock.leaseHoldPreLockedReadLock();
                ReadWriteLock.leaseHoldPreLockedWriteLock();
                //清除所有ThreadLocal
                clearAllThreadLocal();
            }
        });
    }
}
