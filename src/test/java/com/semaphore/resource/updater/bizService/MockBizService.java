package com.semaphore.resource.updater.bizService;

import com.semaphore.resource.updater.core.HighResourceUpdater;
import com.semaphore.resource.updater.core.ResourceUpdater;
import com.semaphore.resource.updater.core.UpdateResourceParam;
import com.semaphore.resource.updater.db.DbAccessor;
import com.semaphore.resource.updater.exceptions.DataUnConsistentException;
import com.semaphore.resource.updater.exceptions.LockWaitException;
import com.semaphore.resource.updater.exceptions.ResourceUpdateException;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 模拟业务操作
 * @date 2022/1/25 2:06 PM
 */
@Service
public class MockBizService {
    private static final String LOCK_WAY_PREFIX = "lock_way_prefix:";
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private DbAccessor dbAccessor;
    @Resource
    @Qualifier("commonResourceUpdater")
    private ResourceUpdater resourceUpdater;
    @Resource
    private HighResourceUpdater highResourceUpdater;

    /**
     * 互斥锁的方式批量扣减商品库存
     * @param updateResourceParamList
     */
    @Transactional(rollbackFor = Exception.class)
    public void trySubtractResourceByMutexLock(List<UpdateResourceParam> updateResourceParamList) throws Exception {
        updateResourceParamList.sort(Comparator.comparing(UpdateResourceParam::getResourceId));
        List<RLock> rLockList = new ArrayList<>();

        try {
            for(UpdateResourceParam param : updateResourceParamList){
                String resourceId = param.getResourceId();
                RLock rLock = redissonClient.getLock(LOCK_WAY_PREFIX + resourceId);
                boolean locked = rLock.tryLock(600,6000, TimeUnit.MILLISECONDS);
                if(locked){
                    rLockList.add(rLock);
                }else {
                    throw new Exception("锁等待超时了");
                }
            }
            //锁全部获取到
            dbAccessor.subtractAvailableResource(updateResourceParamList);
        }catch (Exception e){
            throw e;
        }finally {
            rLockList.forEach(lock -> lock.unlock());
        }
    }

    /**
     * 信号量方式 批量扣减库存
     * @param updateResourceParams
     */
    @Transactional(rollbackFor = Exception.class)
    public void trySubtractResource(HashSet<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        resourceUpdater.trySubtractAvailable(updateResourceParams);
    }

    @Transactional(rollbackFor = Exception.class)
    public void tryAddAvailableResource(HashSet<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        resourceUpdater.addAvailable(updateResourceParams);
    }

    /**
     * 信号量方式 批量扣减可用库存并增加预占库存
     * @param updateResourceParams
     * @throws LockWaitException
     * @throws InterruptedException
     * @throws DataUnConsistentException
     * @throws ResourceUpdateException
     */
    @Transactional(rollbackFor = Exception.class)
    public void trySubtractAvailableAddPreLock(Set<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        highResourceUpdater.trySubtractAvailableAddPreLock(updateResourceParams);
    }

    @Transactional(rollbackFor = Exception.class)
    public void tryRollBackPreLock(Set<UpdateResourceParam> updateResourceParamList)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        highResourceUpdater.trySubtractPreLockAddAvailable(updateResourceParamList);
    }

    @Transactional(rollbackFor = Exception.class)
    public void trySubtractPreLock(HashSet<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        highResourceUpdater.trySubtractPreLock(updateResourceParams);
    }

    @Transactional(rollbackFor = Exception.class)
    public void tryAddPreLockResource(HashSet<UpdateResourceParam> updateResourceParams)
            throws LockWaitException, InterruptedException, DataUnConsistentException, ResourceUpdateException {
        highResourceUpdater.addPreLock(updateResourceParams);
    }

    public void setAutoAdjustRate(int rate){
        resourceUpdater.setAutoAdjustRate(rate);
    }
}
