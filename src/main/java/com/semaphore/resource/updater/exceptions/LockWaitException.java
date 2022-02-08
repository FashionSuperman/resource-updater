package com.semaphore.resource.updater.exceptions;

/**
 * 异常
 *
 * @date 2021/7/22 11:17 上午
 */
public class LockWaitException extends Exception{
    public LockWaitException(String msg){
        super(msg);
    }
}
