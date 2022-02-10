package com.semaphore.resource.updater.cache;

import com.semaphore.resource.updater.core.*;
import com.semaphore.resource.updater.db.DbAccessor;
import com.semaphore.resource.updater.exceptions.LockWaitException;
import com.semaphore.resource.updater.exceptions.ResourceRunException;
import com.semaphore.resource.updater.exceptions.ResourceWaitException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.connection.ConnectionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 * @date 2021/10/14 10:39 上午
 */
@Slf4j
public class CacheAccessor {
    /**
     * 缓存与DB自动调节概率
     * 针对缓存少于数据库的场景
     * 例如 缓存信号量获取时，数量不满足要求或者数量为0
     * 设置为 0-10 之间的整数 设置为0 表示永远不调节；设置为1 表示有10%的概率调节；设置为10 表示一定调节
     * 默认值 3
     */
    private static final String AUTO_ADJUST_RATE = "resource_auto_adjust_rate";
    private static final int DEFAULT_AUTO_ADJUST_RATE = 3;
    private static final Random random = new Random();

    public static final String RESOURCE_AVAILABLE_KEY_PREFIX = "resource_available_key_prefix:";
    public static final String RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX = "resource_available_not_consistence_key_prefix:";

    public static final String RESOURCE_PRE_LOCK_KEY_PREFIX = "resource_pre_lock_key_prefix:";
    public static final String RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX = "resource_pre_lock_not_consistence_key_prefix:";
    public static RedissonClient redissonClient;
    /**
     * 持有(减掉)的可用资源信号量
     * 如果整体失败 需要添加回去
     */
    private static final ThreadLocal<List<SemaphoreAndPermit>> HOLD_AVAILABLE_SEMAPHORE = new ThreadLocal<>();
    /**
     * 持有（减掉）的预占资源信号量
     * 如果整体失败 需要添加回去
     */
    private static final ThreadLocal<List<SemaphoreAndPermit>> HOLD_PRE_LOCK_SEMAPHORE = new ThreadLocal<>();
    /**
     * 增加的可用资源信号量
     * 如果整体失败 需要减掉
     */
    private static final ThreadLocal<List<SemaphoreAndPermit>> LEASED_AVAILABLE_SEMAPHORE = new ThreadLocal<>();
    /**
     * 增加的预占资源信号量
     * 如果整体失败 需要减掉
     */
    private static final ThreadLocal<List<SemaphoreAndPermit>> LEASED_PRE_LOCK_SEMAPHORE = new ThreadLocal<>();

    private static final int LOOP_LIMIT = 10;

    /**
     * 从缓存中读取资源
     * @param resourceId
     * @return
     */
    public static ResourcePermit readResource(String resourceId){
        MySemaphore availableSemaphore = getResourceAvailableSemaphore(resourceId);
        MySemaphore preLockSemaphore = getResourcePreLockSemaphore(resourceId);
        return ResourcePermit
                .builder()
                .resourceId(resourceId)
                .availableCount(availableSemaphore.availablePermits())
                .preLockCount(preLockSemaphore.availablePermits())
                .build();
    }


    //===============================================以下是对于调节概率的操作===============================================

    public static void setAutoAdjustRate(int rate){
        redissonClient.getBucket(AUTO_ADJUST_RATE).set(rate);
    }

    public static int getAutoAdjustRate(){
        Object val = redissonClient.getBucket(AUTO_ADJUST_RATE).get();
        if(Objects.isNull(val)){
            return DEFAULT_AUTO_ADJUST_RATE;
        }else {
            return (int) val;
        }
    }

    public static boolean shouldAdjust(){
        int rate = getAutoAdjustRate();
        int tempRandom = random.nextInt(10);
        if(tempRandom < rate){
            return true;
        }else {
            return false;
        }
    }

    //===============================================以上是对于调节概率的操作===============================================

    //===============================================以下是对于可用资源的操作===============================================

    /**
     * 查询可用资源数量
     * @param queryResourceParam
     * @return
     */
    public static QueryResourceResult queryAvailable(QueryResourceParam queryResourceParam){
        String resourceId = queryResourceParam.getResourceId();
        int acquire = queryResourceParam.getAcquire();
        MySemaphore mySemaphore = getResourceAvailableSemaphore(resourceId);
        if(!mySemaphore.isExists()){
            return null;
        }
        int availablePermit =  mySemaphore.availablePermits();
        boolean fill = availablePermit >= acquire;
        if(availablePermit == 0 || !fill){
            adjustAvailableResource(resourceId);
        }
        return QueryResourceResult.builder().resourceId(resourceId).acquire(acquire).num(availablePermit).fill(fill).build();
    }

    /**
     * 尝试单个获取可用资源permit数量的信号量
     * @param updateResourceParam
     * @param dbAccessor 数据库访问器 用于当缓存中资源不存在时，初始化缓存中的资源
     * @throws ResourceWaitException
     */
    public static void tryAcquireAvailableSemaphore(UpdateResourceParam updateResourceParam, DbAccessor dbAccessor)
            throws ResourceWaitException, LockWaitException, InterruptedException {
        try {
            String notExistResourceId = doTryAcquireAvailable(updateResourceParam);
            if(Objects.nonNull(notExistResourceId)){
                initAvailableSemaphorePermit(notExistResourceId,dbAccessor);
                throw new ResourceWaitException("获取:" + notExistResourceId + "可用资源信号量失败，等待初始化");
            }
        }catch (Exception e){
            leaseAcquiredAvailableSemaphore();
            throw e;
        }
    }

    /**
     * 尝试批量获取可用资源permit数量的信号量
     * @param resourcePermitList
     * @param dbAccessor 数据库访问器 用于当缓存中资源不存在时，初始化缓存中的资源
     * @throws
     */
    public static void tryAcquireAvailableSemaphore(List<UpdateResourceParam> resourcePermitList,DbAccessor dbAccessor)
            throws ResourceWaitException, LockWaitException, InterruptedException {
        try {
            List<String> notExistResourceIdList = doTryAcquireAvailable(resourcePermitList);
            if(Objects.nonNull(notExistResourceIdList) && notExistResourceIdList.size() > 0){
                initAvailableSemaphorePermit(notExistResourceIdList,dbAccessor);
                throw new ResourceWaitException("批量获取可用资源信号量失败，等待初始化");
            }
        }catch (Exception e){
            leaseAcquiredAvailableSemaphore();
            throw e;
        }
    }

    /**
     * 释放持有的可用资源信号量
     */
    public static void leaseAcquiredAvailableSemaphore(){
        doLeaseAcquiredSemaphore(HOLD_AVAILABLE_SEMAPHORE);
    }

    /**
     * 批量初始化可用资源信号量缓存
     * @param resourceIdList
     */
    public static void initAvailableSemaphorePermit(List<String> resourceIdList,DbAccessor dbAccessor) throws LockWaitException, InterruptedException {
        //先释放掉持有的读锁
        ReadWriteLock.leaseHoldAvailableReadLock();
        //加锁
        ReadWriteLock.availableTryLockWrite(resourceIdList);
        try {
            //批量db查询
            List<ResourcePermit> resourcePermitList = dbAccessor.queryResource(resourceIdList);
            //逐个更新缓存信号量
            resourcePermitList.forEach(resourcePermit -> {
                String resourceId = resourcePermit.getResourceId();
                MySemaphore rSemaphore = getResourceAvailableSemaphore(resourceId);
                if(rSemaphore.isExists() && !keyExist(RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX + resourceId)){
                    return;
                }
                trySetPermitsLoop(rSemaphore,resourcePermit.getAvailableCount());
                //删除标记缓存与数据库不一致的redisKey
                deleteAvailableResourceNotConsistence(resourceId);
            });
        }catch (Exception e){
            throw e;
        }finally {
            ReadWriteLock.leaseHoldAvailableWriteLock();
        }
    }

    /**
     * 增加可用数量信号量
     * @param resourceId
     * @param num
     */
    public static void addAvailableResourceSemaphore(String resourceId,int num){
        MySemaphore mySemaphore = getResourceAvailableSemaphore(resourceId);
        if(mySemaphore.isExists()){
            throw new ResourceRunException("增加资源key:" + resourceId + " 可用信号量数量失败，cacheKey不存在");
        }
        mySemaphore.release(num);
    }

    /**
     * 删除可用资源信号量cache
     */
    public static void deleteAvailableResourceSemaphore(String resourceId) throws LockWaitException, InterruptedException {
        ReadWriteLock.leaseHoldAvailableReadLock();
        ReadWriteLock.availableTryLockWrite(resourceId);
        MySemaphore mySemaphore = getResourceAvailableSemaphore(resourceId);
        if(mySemaphore.isExists()){
            mySemaphore.delete();
        }
        //不一致标记也一起删除
        deleteAvailableResourceNotConsistence(resourceId);
        ReadWriteLock.leaseHoldAvailableWriteLock();
    }

    /**
     * 检查缓存与数据库不一致地可用资源key
     * @param updateResourceParamList
     * @return
     */
    public static List<String> checkAvailableResourceConsistence(List<UpdateResourceParam> updateResourceParamList) {
        return checkConsistence(updateResourceParamList, RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX);
    }

    /**
     * 标记给定的可用资源key数据库和缓存不一致
     * @param resourceId
     */
    public static void setAvailableResourceNotConsistence(String resourceId){
        redissonClient.getBucket(RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX + resourceId).set("Not_Consistence");
    }

    /**
     * 批量释放（增加）可用资源信号量
     * @param updateResourceParamList
     * @param dbAccessor
     */
    public static void tryLeaseAvailableSemaphore(List<UpdateResourceParam> updateResourceParamList, DbAccessor dbAccessor)
            throws ResourceWaitException, LockWaitException, InterruptedException {
        try {
            List<String> notExistResourceIdList = doTryLeaseAvailable(updateResourceParamList);
            if(Objects.nonNull(notExistResourceIdList) && notExistResourceIdList.size() > 0){
                //初始化
                initAvailableSemaphorePermit(notExistResourceIdList,dbAccessor);
                throw new ResourceWaitException("批量增加可用资源信号量失败，等待初始化");
            }
        }catch (Exception e){
            //减掉增加的可用资源
            acquireLeasedAvailableSemaphore();
            throw e;
        }
    }

    /**
     * 减掉之前增加（记录在上下文中）的可用资源信号量
     */
    public static void acquireLeasedAvailableSemaphore() {
        doAcquireLeasedSemaphore(LEASED_AVAILABLE_SEMAPHORE);
    }

    /**
     * 检查可用资源信号量是否已经初始化过 否则进行初始化
     * @param updateResourceParamList
     * @param dbAccessor
     */
    public static void checkAvailableSemaphoreInitializedOrInit(List<UpdateResourceParam> updateResourceParamList,DbAccessor dbAccessor)
            throws LockWaitException, InterruptedException {
        RKeys rKeys = redissonClient.getKeys();
        String[] resourceIdArr = new String[updateResourceParamList.size()];
        for (int i = 0; i < updateResourceParamList.size(); i++) {
            resourceIdArr[i] = RESOURCE_AVAILABLE_KEY_PREFIX + updateResourceParamList.get(i).getResourceId();
        }
        long existCount = rKeys.countExists(resourceIdArr);
        if (existCount == updateResourceParamList.size()) {
            return;
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = updateResourceParam.getResourceId();
            MySemaphore rSemaphore = getResourceAvailableSemaphore(resourceId);
            boolean semaphoreKeyExists = rSemaphore.isExists();
            if(!semaphoreKeyExists){
                notExistResourceIdList.add(resourceId);
            }
        }
        if(notExistResourceIdList.size() > 0){
            //尝试初始化
            initAvailableSemaphorePermit(notExistResourceIdList,dbAccessor);
        }
    }

    //===============================================以上是对于可用资源的操作===============================================

    //===============================================以下是对于预占资源的操作===============================================

    /**
     * 查询预占资源数量
     * @param queryResourceParam
     * @return
     */
    public static QueryResourceResult queryPreLocked(QueryResourceParam queryResourceParam) {
        String resourceId = queryResourceParam.getResourceId();
        int acquire = queryResourceParam.getAcquire();
        MySemaphore mySemaphore = getResourcePreLockSemaphore(resourceId);
        if(!mySemaphore.isExists()){
            return null;
        }
        int preLockedPermit = mySemaphore.availablePermits();
        boolean fill = preLockedPermit >= acquire;
        if(preLockedPermit == 0 || !fill){
            adjustPreLockedResource(resourceId);
        }
        return QueryResourceResult.builder().resourceId(resourceId).acquire(acquire).num(preLockedPermit).fill(fill).build();
    }

    /**
     * 尝试单个获取预占资源permit的数量
     * @param updateResourceParam
     * @throws
     */
    public static void tryAcquirePreLockedSemaphore(UpdateResourceParam updateResourceParam,DbAccessor dbAccessor)
            throws ResourceWaitException, LockWaitException, InterruptedException {
        try {
            String notExistResourceId = doTryAcquirePreLocked(updateResourceParam);
            if(Objects.nonNull(notExistResourceId)){
                //初始化
                initPreLockedSemaphorePermit(notExistResourceId,dbAccessor);
                throw new ResourceWaitException("获取:" + notExistResourceId + "预占资源数量信号量失败,等待初始化");
            }
        }catch (Exception e){
            leaseAcquiredPreLockedSemaphore();
            throw e;
        }
    }

    /**
     * 尝试批量获取预占资源permit的数量
     * @param resourcePermitList
     * @throws
     */
    public static void tryAcquirePreLockedSemaphore(List<UpdateResourceParam> resourcePermitList,DbAccessor dbAccessor)
            throws ResourceWaitException, LockWaitException, InterruptedException {
        try {
            List<String> notExistResourceIdList = doTryAcquirePreLocked(resourcePermitList);
            if(Objects.nonNull(notExistResourceIdList) && notExistResourceIdList.size() > 0){
                //初始化
                initPreLockedSemaphorePermit(notExistResourceIdList,dbAccessor);
                throw new ResourceWaitException("批量获取预占资源信号量失败，等待初始化");
            }
        }catch (Exception e){
            leaseAcquiredPreLockedSemaphore();
            throw e;
        }
    }

    /**
     * 增加预占资源的信号量
     * @param updateResourceParamList
     * @param dbAccessor
     */
    public static void tryLeasePreLockedSemaphore(List<UpdateResourceParam> updateResourceParamList,DbAccessor dbAccessor)
            throws LockWaitException, InterruptedException, ResourceWaitException {
        try {
            List<String> notExistResourceIdList = doTryLeasePreLocked(updateResourceParamList);
            if(Objects.nonNull(notExistResourceIdList) && notExistResourceIdList.size() > 0){
                //初始化
                initPreLockedSemaphorePermit(notExistResourceIdList,dbAccessor);
                throw new ResourceWaitException("批量增加预占资源信号量失败，等待初始化");
            }
        }catch (Exception e){
            //减掉增加的预占资源
            acquireLeasedPreLockedSemaphore();
            throw e;
        }
    }

    /**
     * 增加之前扣减（记录在上下文中）的预占资源信号量
     */
    public static void leaseAcquiredPreLockedSemaphore(){
        doLeaseAcquiredSemaphore(HOLD_PRE_LOCK_SEMAPHORE);
    }

    /**
     * 减掉之前增加（记录在上下文中）的预占资源信号量
     */
    public static void acquireLeasedPreLockedSemaphore() {
        doAcquireLeasedSemaphore(LEASED_PRE_LOCK_SEMAPHORE);
    }

    /**
     * 检查缓存与数据库不一致的预占资源key
     * @param updateResourceParamList
     * @return
     */
    public static List<String> checkPreLockedResourceConsistence(List<UpdateResourceParam> updateResourceParamList) {
        return checkConsistence(updateResourceParamList, RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX);
    }

    /**
     * 批量初始化预占资源信号量缓存
     * @param resourceIdList
     */
    public static void initPreLockedSemaphorePermit(List<String> resourceIdList,DbAccessor dbAccessor) throws LockWaitException, InterruptedException {
        //先释放掉持有的读锁
        ReadWriteLock.leaseHoldPreLockedReadLock();
        //加锁
        ReadWriteLock.preLockedTryLockWrite(resourceIdList);
        try {
            //批量db查询
            List<ResourcePermit> resourcePermitList = dbAccessor.queryResource(resourceIdList);
            //逐个更新缓存信号量
            resourcePermitList.forEach(resourcePermit -> {
                String resourceId = resourcePermit.getResourceId();
                MySemaphore rSemaphore = getResourcePreLockSemaphore(resourceId);
                if(rSemaphore.isExists() && !keyExist(RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX + resourceId)){
                    return;
                }
                trySetPermitsLoop(rSemaphore,resourcePermit.getPreLockCount());
                deletePreLockedResourceNotConsistence(resourceId);
            });
        }catch (Exception e){
            throw e;
        }finally {
            ReadWriteLock.leaseHoldPreLockedWriteLock();
        }
    }

    /**
     * 增加预占资源信号量
     * @param resourceId
     * @param num
     */
    public static void addPreLockedResourceSemaphore(String resourceId, int num){
        MySemaphore mySemaphore = getResourcePreLockSemaphore(resourceId);
        if(mySemaphore.isExists()){
            throw new ResourceRunException("增加资源key:" + resourceId + " 预占信号量数量失败，cacheKey不存在");
        }
        mySemaphore.release(num);
    }

    /**
     * 删除预占资源信号量cache
     * @param resourceId
     */
    public static void deletePreLockedResourceSemaphore(String resourceId) throws LockWaitException, InterruptedException {
        //释放持有的读锁
        ReadWriteLock.leaseHoldPreLockedReadLock();
        //加锁
        ReadWriteLock.preLockedTryLockWrite(resourceId);
        MySemaphore mySemaphore = getResourcePreLockSemaphore(resourceId);
        if(mySemaphore.isExists()){
            mySemaphore.delete();
        }
        //不一致标记也一起删除
        deletePreLockedResourceNotConsistence(resourceId);
        ReadWriteLock.leaseHoldPreLockedWriteLock();
    }

    /**
     * 标记给定的预占资源key数据库和缓存不一致
     * @param resourceId
     */
    public static void setPreLockedResourceNotConsistence(String resourceId) {
        redissonClient.getBucket(RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX + resourceId).set("Not_Consistence");
    }

    /**
     * 检查预占资源信号量是否已经初始化 否则进行初始化
     * @param updateResourceParamList
     * @param dbAccessor
     */
    public static void checkPreLockedSemaphoreInitializedOrInit(List<UpdateResourceParam> updateResourceParamList, DbAccessor dbAccessor)
            throws LockWaitException, InterruptedException {
        RKeys rKeys = redissonClient.getKeys();
        String[] resourceIdArr = new String[updateResourceParamList.size()];
        for (int i = 0; i < updateResourceParamList.size(); i++) {
            resourceIdArr[i] = RESOURCE_PRE_LOCK_KEY_PREFIX + updateResourceParamList.get(i).getResourceId();
        }
        long existCount = rKeys.countExists(resourceIdArr);
        if (existCount == updateResourceParamList.size()) {
            return;
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = updateResourceParam.getResourceId();
            MySemaphore rSemaphore = getResourcePreLockSemaphore(resourceId);
            boolean semaphoreKeyExists = rSemaphore.isExists();
            if(!semaphoreKeyExists){
                notExistResourceIdList.add(resourceId);
            }
        }
        if(notExistResourceIdList.size() > 0){
            //尝试初始化
            initPreLockedSemaphorePermit(notExistResourceIdList,dbAccessor);
        }
    }

    //===============================================以上是对于预占资源的操作===============================================

    /**
     * 清除所有的上下文信息
     */
    public static void clearThreadLocal() {
        HOLD_AVAILABLE_SEMAPHORE.remove();
        LEASED_AVAILABLE_SEMAPHORE.remove();
        HOLD_PRE_LOCK_SEMAPHORE.remove();
        LEASED_PRE_LOCK_SEMAPHORE.remove();
    }

    //=================
    //===============================================以上私有方法===============================================
    //=================

    /**
     * 单个初始化可用资源信号量缓存
     * 通过数据库查询原始值
     * @param resourceId
     */
    private static void initAvailableSemaphorePermit(String resourceId,DbAccessor dbAccessor) throws LockWaitException, InterruptedException {
        //先释放掉持有的读锁
        ReadWriteLock.leaseHoldAvailableReadLock();
        //加锁
        ReadWriteLock.availableTryLockWrite(resourceId);
        try {
            MySemaphore rSemaphore = getResourceAvailableSemaphore(resourceId);
            if(rSemaphore.isExists() && !keyExist(RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX + resourceId)){
                return;
            }
            ResourcePermit resourcePermit = dbAccessor.queryOneResource(resourceId);
            trySetPermitsLoop(rSemaphore,resourcePermit.getAvailableCount());
            deleteAvailableResourceNotConsistence(resourceId);
        }catch (Exception e){
            ReadWriteLock.leaseHoldAvailableWriteLock();
            throw e;
        }
    }

    /**
     * 删除给定的可用资源key的缓存与数据库不一致标记
     * @param resourceId
     */
    private static void deleteAvailableResourceNotConsistence(String resourceId){
        redissonClient.getKeys().delete(RESOURCE_AVAILABLE_NOT_CONSISTENCE_KEY_PREFIX + resourceId);
    }

    /**
     * 验证缓存key是否存在
     * @param resourceId
     * @return
     */
    private static boolean keyExist(String resourceId){
        return redissonClient.getKeys().countExists(resourceId) > 0;
    }

    /**
     * 单个初始化预占资源信号量缓存
     * 通过数据库查询原始值
     * @param resourceId
     */
    private static void initPreLockedSemaphorePermit(String resourceId,DbAccessor dbAccessor) throws LockWaitException, InterruptedException {
        //先释放掉持有的读锁
        ReadWriteLock.leaseHoldPreLockedReadLock();
        //加锁
        ReadWriteLock.preLockedTryLockWrite(resourceId);
        try {
            MySemaphore rSemaphore = getResourcePreLockSemaphore(resourceId);
            if(rSemaphore.isExists() && !keyExist(RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX + resourceId)){
                return;
            }
            ResourcePermit resourcePermit = dbAccessor.queryOneResource(resourceId);
            trySetPermitsLoop(rSemaphore,resourcePermit.getPreLockCount());
        }catch (Exception e){
            ReadWriteLock.leaseHoldPreLockedWriteLock();
            throw e;
        }
    }

    /**
     * 删除给定的预占资源key的缓存与数据库不一致标记
     * @param resourceId
     */
    private static void deletePreLockedResourceNotConsistence(String resourceId) {
        redissonClient.getKeys().delete(RESOURCE_PRE_LOCK_NOT_CONSISTENCE_KEY_PREFIX + resourceId);
    }

    /**
     * 获取可用资源信号量（基于redisson实现）对象
     * @param resourceId
     * @return
     */
    private static MySemaphore getResourceAvailableSemaphore(String resourceId){
        Redisson redisson = (Redisson) redissonClient;
        ConnectionManager connectionManager = redisson.getConnectionManager();
        MySemaphore rSemaphore = new MySemaphore(connectionManager.getCommandExecutor(), RESOURCE_AVAILABLE_KEY_PREFIX + resourceId);
        return rSemaphore;
    }

    /**
     * 获取预占资源信号量（基于redisson实现）对象
     * @param resourceId
     * @return
     */
    private static MySemaphore getResourcePreLockSemaphore(String resourceId){
        Redisson redisson = (Redisson) redissonClient;
        ConnectionManager connectionManager = redisson.getConnectionManager();
        MySemaphore rSemaphore = new MySemaphore(connectionManager.getCommandExecutor(), RESOURCE_PRE_LOCK_KEY_PREFIX + resourceId);
        return rSemaphore;
    }

    /**
     * 单个尝试获取（减掉）可用资源信号量
     * 返回不存在的Semaphore列表
     * 返回null时表示Semaphore全部存在，并且信号量全部获取成功
     * @param updateResourceParamList
     * @return
     * @throws ResourceWaitException
     */
    private static List<String> doTryAcquireAvailable(List<UpdateResourceParam> updateResourceParamList) throws ResourceWaitException {
        if(Objects.isNull(updateResourceParamList)){
            throw new ResourceRunException("请求可用资源resourcePermitList不能为空");
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        try {
            for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                String notExistResourceId = doTryAcquireAvailable(updateResourceParam);
                if(Objects.nonNull(notExistResourceId)){
                    notExistResourceIdList.add(notExistResourceId);
                }
            }
            if(notExistResourceIdList.size() > 0){
                return notExistResourceIdList;
            }else {
                return null;
            }
        }catch (Exception e){
            throw e;
        }
    }

    /**
     * 单个尝试获取（减掉）可用资源信号量
     * 返回不存在的resourceId
     * 返回null时表示Semaphore存在，并且信号量获取成功
     * @param updateResourceParam
     * @return
     * @throws ResourceWaitException
     */
    private static String doTryAcquireAvailable(UpdateResourceParam updateResourceParam) throws ResourceWaitException {
        if(Objects.isNull(updateResourceParam)){
            throw new ResourceRunException("请求可用资源updateResourceParam不能为空");
        }
        String resourceId = updateResourceParam.getResourceId();
        int requireNum = updateResourceParam.getNum();
        if(requireNum < 0){
            throw new ResourceRunException("请求可用资源数量不能小于0");
        }
        MySemaphore rSemaphore = getResourceAvailableSemaphore(resourceId);
        boolean semaphoreKeyExists = rSemaphore.isExists();
        if(!semaphoreKeyExists){
            return resourceId;
        }else {
            //存在，校验数量是否为0
            int availablePermit = rSemaphore.availablePermits();
            if(availablePermit == 0){
                adjustAvailableResource(resourceId);
                throw new ResourceWaitException("获取:" + resourceId + "可用资源信号量为0");
            }
        }
        try {
            boolean acquired = rSemaphore.tryAcquire(requireNum, Const.semaphoreWaitTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(acquired){
                //记录持有的信号量
                recordHoldAvailableSemaphore(rSemaphore,requireNum);
                return null;
            }
            adjustAvailableResource(resourceId);
            throw new ResourceWaitException("获取:" + resourceId + "可用资源信号量超时,资源不足,requireNum:" + requireNum + "remainNum:" + rSemaphore.availablePermits());
        } catch (InterruptedException e) {
            throw new ResourceRunException("获取:" + resourceId + "可用资源信号量中断");
        }
    }

    /**
     * 自动调节可用资源数据库与缓存一致
     * @param resourceId
     */
    private static void adjustAvailableResource(String resourceId) {
        if(shouldAdjust()){
            log.info("可用资源:" + resourceId + "自动调节概率匹配，将自动调节......");
            setAvailableResourceNotConsistence(resourceId);
        }
    }

    /**
     * 批量尝试释放（增加）预占资源的信号量
     * 返回null表示操作成功
     * 否则返回不存在的缓存资源key
     * @param updateResourceParamList
     * @return
     */
    private static List<String> doTryLeasePreLocked(List<UpdateResourceParam> updateResourceParamList) {
        if(Objects.isNull(updateResourceParamList)){
            throw new ResourceRunException("请求增加预占资源resourcePermitList不能为空");
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = doTryLeasePreLocked(updateResourceParam);
            if(Objects.nonNull(resourceId)){
                notExistResourceIdList.add(resourceId);
            }
        }
        if(notExistResourceIdList.size() > 0){
            return notExistResourceIdList;
        }else {
            return null;
        }
    }

    /**
     * 单个尝试释放（增加）预占资源的信号量
     * 返回null表示操作成功
     * 否则返回不存在的缓存资源key
     * @param updateResourceParam
     * @return
     */
    private static String doTryLeasePreLocked(UpdateResourceParam updateResourceParam) {
        if(Objects.isNull(updateResourceParam)){
            throw new ResourceRunException("请求增加预占资源updateResourceParam不能为空");
        }
        String resourceId = updateResourceParam.getResourceId();
        int requireNum = updateResourceParam.getNum();
        if(requireNum < 0){
            throw new ResourceRunException("请求增加预占资源数量不能小于0");
        }
        //因为是增加 不需要验证数量
        MySemaphore rSemaphore = getResourcePreLockSemaphore(resourceId);
        if(!rSemaphore.isExists()){
            return resourceId;
        }
        rSemaphore.release(requireNum);
        //增加成功 上下文中记录此次的增加
        recordLeasedPreLockSemaphore(rSemaphore,requireNum);
        return null;
    }

    /**
     * 单个尝试获取（减掉）可用资源信号量
     * 返回resourceId表示该id没有初始化
     * 返回null表示获取信号量成功
     * @param updateResourceParam
     * @return
     * @throws ResourceWaitException
     */
    private static String doTryAcquirePreLocked(UpdateResourceParam updateResourceParam) throws ResourceWaitException {
        if(Objects.isNull(updateResourceParam)){
            throw new ResourceRunException("请求预占资源updateResourceParam不能为空");
        }
        String resourceId = updateResourceParam.getResourceId();
        int requireNum = updateResourceParam.getNum();
        if(requireNum < 0){
            throw new ResourceRunException("请求预占资源数量不能小于0");
        }
        MySemaphore rSemaphore = getResourcePreLockSemaphore(resourceId);
        boolean exists = rSemaphore.isExists();
        if(!exists){
            return resourceId;
        }
        try {
            boolean acquired = rSemaphore.tryAcquire(requireNum, Const.semaphoreWaitTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(acquired){
                //记录持有的信号量
                recordHoldPreLockSemaphore(rSemaphore,requireNum);
                return null;
            }
            adjustPreLockedResource(resourceId);
            throw new ResourceWaitException("获取:" + resourceId + "预占资源数量信号量超时,资源不足,requireNum:" + requireNum + "remainNum:" + rSemaphore.availablePermits());
        } catch (InterruptedException e) {
            throw new ResourceRunException("获取:" + resourceId + "预占资源信号量中断");
        }
    }

    /**
     * 自动调节预占资源数据库与缓存一致
     * @param resourceId
     */
    private static void adjustPreLockedResource(String resourceId) {
        if(shouldAdjust()){
            log.info("预占资源:" + resourceId + "自动调节概率匹配，将自动调节......");
            setPreLockedResourceNotConsistence(resourceId);
        }
    }

    /**
     * 批量尝试获取（减掉）预占资源信号量
     * 返回resourceId表示该id没有初始化
     * 返回null表示获取信号量成功
     * @param resourcePermitList
     * @return
     * @throws ResourceWaitException
     */
    private static List<String> doTryAcquirePreLocked(List<UpdateResourceParam> resourcePermitList) throws ResourceWaitException {
        if(Objects.isNull(resourcePermitList)){
            throw new ResourceRunException("请求预占资源resourcePermitList不能为空");
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        try {
            for(UpdateResourceParam updateResourceParam : resourcePermitList){
                String notExistResourceId = doTryAcquirePreLocked(updateResourceParam);
                if(Objects.nonNull(notExistResourceId)){
                    notExistResourceIdList.add(notExistResourceId);
                }
            }
            if(notExistResourceIdList.size() > 0){
                return notExistResourceIdList;
            }else {
                return null;
            }
        }catch (Exception e){
            throw e;
        }
    }

    /**
     * 初始化某个信号量为指定的数值
     * @param rSemaphore
     * @param count
     */
    private static void trySetPermitsLoop(MySemaphore rSemaphore,int count){
        for(int i=0 ; i<LOOP_LIMIT ; i++){
            boolean setResult = rSemaphore.trySetPermitsForce(count);
            if(setResult){
                return;
            }
        }
        throw new ResourceRunException("更新资源:" + rSemaphore.getName() + "缓存信号量失败，请稍后重试");
    }

    /**
     * 释放（增加）掉上下文中记录的持有（减掉）的某类信号量
     * @param threadLocal
     */
    private static void doLeaseAcquiredSemaphore(ThreadLocal<List<SemaphoreAndPermit>> threadLocal) {
        List<SemaphoreAndPermit> semaphoreAndPermitList = threadLocal.get();
        if(Objects.isNull(semaphoreAndPermitList)){
            return;
        }
        semaphoreAndPermitList.forEach(semaphoreAndPermit -> {
            MySemaphore rSemaphore = semaphoreAndPermit.getRSemaphore();
            if(!rSemaphore.isExists()){
                return;
            }
            rSemaphore.release(semaphoreAndPermit.getPermit());
        });
        threadLocal.remove();
    }

    /**
     * 上下文中记录持有（减掉）的可用资源信号量
     * @param rSemaphore
     * @param permit
     */
    private static void recordHoldAvailableSemaphore(MySemaphore rSemaphore, int permit){
        List<SemaphoreAndPermit> semaphoreAndPermitList = HOLD_AVAILABLE_SEMAPHORE.get();
        if(Objects.isNull(semaphoreAndPermitList)){
            semaphoreAndPermitList = new ArrayList<>();
            HOLD_AVAILABLE_SEMAPHORE.set(semaphoreAndPermitList);
        }
        semaphoreAndPermitList.add(SemaphoreAndPermit.builder().rSemaphore(rSemaphore).permit(permit).build());
    }

    /**
     * 上下文中记录持有（减掉）的预占资源信号量
     * @param rSemaphore
     * @param permit
     */
    private static void recordHoldPreLockSemaphore(MySemaphore rSemaphore, int permit){
        List<SemaphoreAndPermit> semaphoreAndPermitList = HOLD_PRE_LOCK_SEMAPHORE.get();
        if(Objects.isNull(semaphoreAndPermitList)){
            semaphoreAndPermitList = new ArrayList<>();
            HOLD_PRE_LOCK_SEMAPHORE.set(semaphoreAndPermitList);
        }
        semaphoreAndPermitList.add(SemaphoreAndPermit.builder().rSemaphore(rSemaphore).permit(permit).build());
    }

    /**
     * 上下文中记录释放（增加）的预占资源信号量
     * @param rSemaphore
     * @param permit
     */
    private static void recordLeasedPreLockSemaphore(MySemaphore rSemaphore, int permit){
        List<SemaphoreAndPermit> leasedSemaphoreList = LEASED_PRE_LOCK_SEMAPHORE.get();
        if(Objects.isNull(leasedSemaphoreList)){
            leasedSemaphoreList = new ArrayList<>();
            LEASED_PRE_LOCK_SEMAPHORE.set(leasedSemaphoreList);
        }
        leasedSemaphoreList.add(SemaphoreAndPermit.builder().rSemaphore(rSemaphore).permit(permit).build());
    }

    /**
     * 检查给定的资源类型前缀和资源key组成的资源 是否缓存和数据库不一致
     * 如果存在不一致 返回不一致的资源key
     * @param updateResourceParamList
     * @param resourcePreLockNotConsistenceKeyPrefix
     * @return
     */
    private static List<String> checkConsistence(List<UpdateResourceParam> updateResourceParamList, String resourcePreLockNotConsistenceKeyPrefix) {
        RKeys rKeys = redissonClient.getKeys();
        String[] resourceIdArr = new String[updateResourceParamList.size()];
        for (int i = 0; i < updateResourceParamList.size(); i++) {
            resourceIdArr[i] = resourcePreLockNotConsistenceKeyPrefix + updateResourceParamList.get(i).getResourceId();
        }
        long existCount = rKeys.countExists(resourceIdArr);
        if (existCount == 0) {
            return null;
        }
        //存在不一致的key,找出不一致的Key
        List<String> notConsistenceList = new ArrayList<>();
        for (int i = 0; i < resourceIdArr.length; i++) {
            String key = resourceIdArr[i];
            if (rKeys.countExists(key) > 0) {
                notConsistenceList.add(key.replaceFirst(resourcePreLockNotConsistenceKeyPrefix, ""));
            }
        }
        return notConsistenceList;
    }

    /**
     * 减掉之前增加（记录在上下文中）的某种信号量
     * @param threadLocal
     */
    private static void doAcquireLeasedSemaphore(ThreadLocal<List<SemaphoreAndPermit>> threadLocal) {
        List<SemaphoreAndPermit> leasedSemaphoreList = threadLocal.get();
        if(Objects.isNull(leasedSemaphoreList)){
            return;
        }
        for (SemaphoreAndPermit semaphoreAndPermit : leasedSemaphoreList) {
            MySemaphore rSemaphore = semaphoreAndPermit.getRSemaphore();
            if (!rSemaphore.isExists()) {
                continue;
            }
            try {
                rSemaphore.acquire(semaphoreAndPermit.getPermit());
            } catch (InterruptedException e) {
                log.warn("正在减掉增加的资源，不能响应中断");
            } catch (Exception e){
                log.warn("正在减掉增加的资源，发生异常:{}，资源:{} 处理失败，之后的资源（上下文中）将继续处理",e.getMessage(),rSemaphore.getResourceId());
            }
        }
        threadLocal.remove();
    }

    /**
     * 批量尝试释放（增加）可用资源信号量
     * 返回resourceId表示该id没有初始化
     * 返回null表示释放信号量成功
     * @param updateResourceParamList
     * @return
     */
    private static List<String> doTryLeaseAvailable(List<UpdateResourceParam> updateResourceParamList) {
        if(Objects.isNull(updateResourceParamList)){
            throw new ResourceRunException("请求增加可用资源resourcePermitList不能为空");
        }
        List<String> notExistResourceIdList = new ArrayList<>();
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = doTryLeaseAvailable(updateResourceParam);
            if(Objects.nonNull(resourceId)){
                notExistResourceIdList.add(resourceId);
            }
        }
        if(notExistResourceIdList.size() > 0){
            return notExistResourceIdList;
        }else {
            return null;
        }
    }

    /**
     * 单个尝试释放（增加）可用资源信号量
     * 返回resourceId表示该id没有初始化
     * 返回null表示释放信号量成功
     * @param updateResourceParam
     * @return
     */
    private static String doTryLeaseAvailable(UpdateResourceParam updateResourceParam) {
        if(Objects.isNull(updateResourceParam)){
            throw new ResourceRunException("请求增加可用资源updateResourceParam不能为空");
        }
        String resourceId = updateResourceParam.getResourceId();
        int requireNum = updateResourceParam.getNum();
        if(requireNum < 0){
            throw new ResourceRunException("请求增加可用资源数量不能小于0");
        }
        //因为是增加 不需要验证数量
        MySemaphore rSemaphore = getResourceAvailableSemaphore(resourceId);
        if(!rSemaphore.isExists()){
            return resourceId;
        }
        rSemaphore.release(requireNum);
        //增加成功 上下文中记录此次的增加
        recordLeasedAvailableSemaphore(rSemaphore,requireNum);
        return null;
    }

    /**
     * 上下文中记录释放（增加）的可用资源信号量
     * @param rSemaphore
     * @param permit
     */
    private static void recordLeasedAvailableSemaphore(MySemaphore rSemaphore, int permit) {
        List<SemaphoreAndPermit> leasedSemaphoreList = LEASED_AVAILABLE_SEMAPHORE.get();
        if(Objects.isNull(leasedSemaphoreList)){
            leasedSemaphoreList = new ArrayList<>();
            LEASED_AVAILABLE_SEMAPHORE.set(leasedSemaphoreList);
        }
        leasedSemaphoreList.add(SemaphoreAndPermit.builder().rSemaphore(rSemaphore).permit(permit).build());
    }
}
