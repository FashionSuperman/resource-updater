package com.semaphore.resource.updater.cache;

import org.redisson.RedissonSemaphore;
import org.redisson.api.RFuture;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;

import java.util.Arrays;

/**
 *
 * @date 2021/10/14 6:44 下午
 */
public class MySemaphore extends RedissonSemaphore {
    private static final String COLON = ":";
    final CommandAsyncExecutor commandExecutor;

    public MySemaphore(CommandAsyncExecutor commandExecutor, String name){
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
    }

    public boolean trySetPermitsForce(int permits){
        return get(trySetPermitsAsyncForce(permits));
    }

    private RFuture<Boolean> trySetPermitsAsyncForce(int permits) {
        String channelName = getChannelName(getName());
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local value = redis.call('get', KEYS[1]); " +
                        "if (value == false or tonumber(value) >= 0) then "
                        + "redis.call('set', KEYS[1], ARGV[1]); "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1;"
                        + "end;"
                        + "return 0;",
                Arrays.<Object>asList(getName(), channelName), permits);
    }

    public String getResourceId(){
        String semaphoreName = getName();
        String[] semaphoreNameArr = semaphoreName.split(COLON);
        return semaphoreNameArr[semaphoreNameArr.length-1];
    }
}
