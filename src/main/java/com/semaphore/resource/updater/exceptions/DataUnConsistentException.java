package com.semaphore.resource.updater.exceptions;

/**
 * 数据不一致异常
 * cache db 不一致
 *
 * @date 2021/7/22 11:17 上午
 */
public class DataUnConsistentException extends Exception{
    public DataUnConsistentException(String msg){
        super(msg);
    }
}
