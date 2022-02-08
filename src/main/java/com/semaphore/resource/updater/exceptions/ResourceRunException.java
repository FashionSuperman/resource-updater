package com.semaphore.resource.updater.exceptions;

/**
 * 运行异常
 *
 * @date 2021/7/22 11:17 上午
 */
public class ResourceRunException extends RuntimeException{
    public ResourceRunException(String msg){
        super(msg);
    }
}
