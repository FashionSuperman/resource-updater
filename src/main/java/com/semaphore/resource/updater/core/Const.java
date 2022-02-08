package com.semaphore.resource.updater.core;

/**
 *
 * @date 2021/10/14 2:58 下午
 */
public class Const {
    /**
     * 信号量等待超时时间 毫秒
     */
    public static long semaphoreWaitTimeMilliSecond = 300;

    /**
     * 读锁等待超时时间 毫秒
     */
    public static long semaphoreReadLockWaitTimeMilliSecond = 300;

    /**
     * 写锁等待超时时间 毫秒
     */
    public static long semaphoreWriteLockWaitTimeMilliSecond = 1000;

    /**
     * 锁默认释放时间 毫秒
     */
    public static long semaphoreLockLeaseTimeMilliSecond = 60000;
}
