package com.semaphore.resource.updater.core;

import com.semaphore.resource.updater.exceptions.LockWaitException;
import com.semaphore.resource.updater.cache.CacheAccessor;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 *
 * @date 2021/10/18 8:30 下午
 */
public class ReadWriteLock {
    public static final String AVAILABLE_SEMAPHORE_LOCK_PREFIX = "available_semaphore_lock_prefix:";
    public static final String PRE_LOCKED_SEMAPHORE_LOCK_PREFIX = "pre_locked_semaphore_lock_prefix:";

    private static final ThreadLocal<List<RLock>> HOLD_AVAILABLE_READ_LOCK = new ThreadLocal<>();
    private static final ThreadLocal<List<RLock>> HOLD_PRE_LOCKED_READ_LOCK = new ThreadLocal<>();

    private static final ThreadLocal<List<RLock>> HOLD_AVAILABLE_WRITE_LOCK = new ThreadLocal<>();
    private static final ThreadLocal<List<RLock>> HOLD_PRE_LOCKED_WRITE_LOCK = new ThreadLocal<>();

    public static void availableTryLockWrite(String resourceId) throws LockWaitException, InterruptedException {
        RReadWriteLock rReadWriteLock = CacheAccessor.redissonClient.getReadWriteLock(AVAILABLE_SEMAPHORE_LOCK_PREFIX + resourceId);
        RLock writeLock = rReadWriteLock.writeLock();
        try {
            boolean locked = writeLock.tryLock(Const.semaphoreWriteLockWaitTimeMilliSecond, Const.semaphoreLockLeaseTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(locked){
                //记录持有的写锁
                addHoldAvailableWriteLock(writeLock);
                return;
            }else {
                throw new LockWaitException("资源:" + resourceId + "可用数量信号量写锁等待超时");
            }
        } catch (Exception e) {
            //释放掉持有的写锁
            leaseHoldAvailableWriteLock();
            throw e;
        }
    }

    public static void availableTryLockWrite(List<String> resourceIdList) throws LockWaitException, InterruptedException {
        try {
            for(String resourceId : resourceIdList){
                availableTryLockWrite(resourceId);
            }
        }catch (Exception e){
            leaseHoldAvailableWriteLock();
            throw e;
        }
    }

    public static void preLockedTryLockWrite(String resourceId) throws LockWaitException, InterruptedException {
        RReadWriteLock rReadWriteLock = CacheAccessor.redissonClient.getReadWriteLock(PRE_LOCKED_SEMAPHORE_LOCK_PREFIX + resourceId);
        RLock writeLock = rReadWriteLock.writeLock();
        try {
            boolean locked = writeLock.tryLock(Const.semaphoreWriteLockWaitTimeMilliSecond, Const.semaphoreLockLeaseTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(locked){
                //记录持有的写锁
                addHoldPreLockedWriteLock(writeLock);
                return;
            }else {
                throw new LockWaitException("资源:" + resourceId + "预占数量信号量写锁等待超时");
            }
        } catch (Exception e) {
            //释放掉持有的写锁
            leaseHoldPreLockedWriteLock();
            throw e;
        }
    }

    public static void preLockedTryLockWrite(List<String> resourceIdList) throws LockWaitException, InterruptedException {
        try {
            for(String resourceId : resourceIdList){
                preLockedTryLockWrite(resourceId);
            }
        }catch (Exception e){
            leaseHoldPreLockedWriteLock();
            throw e;
        }
    }

    public static void availableTryLockRead(String resourceId) throws LockWaitException, InterruptedException {
        RReadWriteLock rReadWriteLock = CacheAccessor.redissonClient.getReadWriteLock(AVAILABLE_SEMAPHORE_LOCK_PREFIX + resourceId);
        RLock readLock = rReadWriteLock.readLock();
        try {
            boolean locked = readLock.tryLock(Const.semaphoreReadLockWaitTimeMilliSecond, Const.semaphoreLockLeaseTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(locked){
                //记录持有的读锁
                addHoldAvailableReadLock(readLock);
                return;
            }else {
                throw new LockWaitException("资源:" + resourceId + "可用数量信号量读锁等待超时");
            }
        } catch (Exception e) {
            //释放掉持有的写锁
            leaseHoldAvailableReadLock();
            throw e;
        }
    }

    public static void availableTryLockRead(List<String> resourceIdList) throws LockWaitException, InterruptedException {
        try {
            for(String resourceId : resourceIdList){
                availableTryLockRead(resourceId);
            }
        }catch (Exception e){
            leaseHoldAvailableReadLock();
            throw e;
        }
    }

    public static void preLockedTryLockRead(String resourceId) throws LockWaitException, InterruptedException {
        RReadWriteLock rReadWriteLock = CacheAccessor.redissonClient.getReadWriteLock(PRE_LOCKED_SEMAPHORE_LOCK_PREFIX + resourceId);
        RLock readLock = rReadWriteLock.readLock();
        try {
            boolean locked = readLock.tryLock(Const.semaphoreReadLockWaitTimeMilliSecond, Const.semaphoreLockLeaseTimeMilliSecond, TimeUnit.MILLISECONDS);
            if(locked){
                //记录持有的读锁
                addHoldPreLockedReadLock(readLock);
                return;
            }else {
                throw new LockWaitException("资源:" + resourceId + "预占数量信号量读锁等待超时");
            }
        } catch (Exception e) {
            //释放掉持有的写锁
            leaseHoldPreLockedReadLock();
            throw e;
        }
    }

    public static void preLockedTryLockRead(List<String> resourceIdList) throws LockWaitException, InterruptedException {
        try {
            for(String resourceId : resourceIdList){
                preLockedTryLockRead(resourceId);
            }
        }catch (Exception e){
            leaseHoldPreLockedReadLock();
            throw e;
        }
    }

    //======
    //======
    //======

    private static void addHoldAvailableWriteLock(RLock writeLock){
        List<RLock> holdWriteLock = HOLD_AVAILABLE_WRITE_LOCK.get();
        if(Objects.isNull(holdWriteLock)){
            holdWriteLock = new ArrayList<>();
            HOLD_AVAILABLE_WRITE_LOCK.set(holdWriteLock);
        }
        holdWriteLock.add(writeLock);
    }

    public static void leaseHoldAvailableWriteLock(){
        List<RLock> holdWriteLock = HOLD_AVAILABLE_WRITE_LOCK.get();
        if(Objects.isNull(holdWriteLock)){
            return;
        }
        holdWriteLock.forEach(writeLock -> writeLock.unlock());
        HOLD_AVAILABLE_WRITE_LOCK.remove();
    }

    private static void addHoldPreLockedWriteLock(RLock writeLock){
        List<RLock> holdWriteLock = HOLD_PRE_LOCKED_WRITE_LOCK.get();
        if(Objects.isNull(holdWriteLock)){
            holdWriteLock = new ArrayList<>();
            HOLD_PRE_LOCKED_WRITE_LOCK.set(holdWriteLock);
        }
        holdWriteLock.add(writeLock);
    }

    public static void leaseHoldPreLockedWriteLock(){
        List<RLock> holdWriteLock = HOLD_PRE_LOCKED_WRITE_LOCK.get();
        if(Objects.isNull(holdWriteLock)){
            return;
        }
        holdWriteLock.forEach(writeLock -> writeLock.unlock());
        HOLD_PRE_LOCKED_WRITE_LOCK.remove();
    }

    private static void addHoldAvailableReadLock(RLock readLock){
        List<RLock> holdReadLock = HOLD_AVAILABLE_READ_LOCK.get();
        if(Objects.isNull(holdReadLock)){
            holdReadLock = new ArrayList<>();
            HOLD_AVAILABLE_READ_LOCK.set(holdReadLock);
        }
        holdReadLock.add(readLock);
    }

    public static void leaseHoldAvailableReadLock(){
        List<RLock> holdReadLock = HOLD_AVAILABLE_READ_LOCK.get();
        if(Objects.isNull(holdReadLock)){
            return;
        }
        holdReadLock.forEach(readLock -> readLock.unlock());
        HOLD_AVAILABLE_READ_LOCK.remove();
    }

    private static void addHoldPreLockedReadLock(RLock readLock){
        List<RLock> holdReadLock = HOLD_PRE_LOCKED_READ_LOCK.get();
        if(Objects.isNull(holdReadLock)){
            holdReadLock = new ArrayList<>();
            HOLD_PRE_LOCKED_READ_LOCK.set(holdReadLock);
        }
        holdReadLock.add(readLock);
    }

    public static void leaseHoldPreLockedReadLock(){
        List<RLock> holdReadLock = HOLD_PRE_LOCKED_READ_LOCK.get();
        if(Objects.isNull(holdReadLock)){
            return;
        }
        holdReadLock.forEach(readLock -> readLock.unlock());
        HOLD_PRE_LOCKED_READ_LOCK.remove();
    }

    public static void clearThreadLocal() {
        HOLD_AVAILABLE_READ_LOCK.remove();
        HOLD_PRE_LOCKED_READ_LOCK.remove();
        HOLD_AVAILABLE_WRITE_LOCK.remove();
        HOLD_PRE_LOCKED_WRITE_LOCK.remove();
    }
}
