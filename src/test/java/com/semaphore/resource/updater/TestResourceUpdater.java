package com.semaphore.resource.updater;

import com.google.common.collect.Lists;
import com.semaphore.resource.updater.base.BaseSpringTest;
import com.semaphore.resource.updater.core.*;
import com.semaphore.resource.updater.bizService.MockBizService;
import com.semaphore.resource.updater.exceptions.LockWaitException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 测试不支持预占的资源操作
 *
 * @date 2022/1/25 2:47 PM
 */
public class TestResourceUpdater extends BaseSpringTest {
    private static final List<String> ProductList = Lists.newArrayList("res1","res2","res3","res4","res5","res6");
    private static final Random random = new Random();
    private static final Map<String, AtomicInteger> AtomicIntegerMap = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> AddedAtomicIntegerMap = new ConcurrentHashMap<>();
    static {
        for (String resKey : ProductList){
            AtomicIntegerMap.putIfAbsent(resKey,new AtomicInteger());
            AddedAtomicIntegerMap.putIfAbsent(resKey,new AtomicInteger());
        }
    }

    @Resource
    private MockBizService mockBizService;

    @Resource
    @Qualifier("commonResourceUpdater")
    private ResourceUpdater resourceUpdater;
    @Resource
    private HighResourceUpdater highResourceUpdater;

    @Test
    public void testSetAutoAdjustRate(){
        mockBizService.setAutoAdjustRate(10);
    }

    @Test
    public void testSubtractAvailable(){
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        UpdateResourceParam param1 = UpdateResourceParam.builder().resourceId("res1").num(3).build();
        UpdateResourceParam param2 = UpdateResourceParam.builder().resourceId("res2").num(6).build();
        UpdateResourceParam param3 = UpdateResourceParam.builder().resourceId("res3").num(9).build();
        updateResourceParamList.add(param1);
        updateResourceParamList.add(param2);
        updateResourceParamList.add(param3);
        try {
            mockBizService.trySubtractResource(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
        }
    }

    /**
     * 开启多个线程测试批量扣减库存
     * 支持两种方式
     * useMutexLock : false 使用该项目提供的信号量方式
     * useMutexLock : true 使用传统的互斥锁方式
     *
     * 运行之前 需要初始化表 并在表中初始化库存 "res1","res2","res3","res4","res5","res6"
     * @throws InterruptedException
     */
    @Test
    public void testTrySubtractAvailableMultiThread() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int threadNum = 80;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i=0 ; i<threadNum ;i++){
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int i=0 ; i<1000 ;i++){
                        try {
                            testTrySubtractAvailable(false);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.println("线程:" + Thread.currentThread() + "完成，用时: " + (System.currentTimeMillis() - startTime)/1000 + " 秒");
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        countDownLatch.await();
        //全部完成，输出使用的资源数量
        AtomicIntegerMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共消耗数量: " + entry.getValue()));
        System.out.println("总用时:" + (System.currentTimeMillis() - startTime)/1000 + "秒");
    }

    /**
     * 测试 多线程扣减可用库存的同时多线程增加库存
     */
    @Test
    public void testTrySubtractAndAddAvailableMultiThread() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int threadNum1 = 60;
        int threadNum2 = 20;
        CountDownLatch countDownLatch = new CountDownLatch(threadNum1 + threadNum2);
        //扣减库存的线程
        for(int i=0 ; i<threadNum1 ;i++){
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int i=0 ; i<1000 ;i++){
                        try {
                            testTrySubtractAvailable(false);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.println("扣减线程:" + Thread.currentThread() + "完成，用时: " + (System.currentTimeMillis() - startTime)/1000 + " 秒");
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        //增加库存的线程
        for(int i=0 ; i<threadNum2 ;i++){
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int i=0 ; i<1000 ;i++){
                        try {
                            testTryAddAvailable();
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.println("增加线程:" + Thread.currentThread() + "完成，用时: " + (System.currentTimeMillis() - startTime)/1000 + " 秒");
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }

        countDownLatch.await();
        //全部完成，输出使用的资源数量
        AtomicIntegerMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共消耗数量: " + entry.getValue()));
        AddedAtomicIntegerMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共添加数量: " + entry.getValue()));
        System.out.println("总用时:" + (System.currentTimeMillis() - startTime)/1000 + "秒");
    }

    /**
     * 测试 扣减可用资源
     * 逻辑：
     * 随机选择n个商品 进行批量扣减库存（要扣减的库存数也随机生成）
     */
    private void testTrySubtractAvailable(boolean useMutexLock){
        //随机商品个数
        int allCount = ProductList.size();
        int willLockNum = random.nextInt(allCount);
        willLockNum = willLockNum + 1;
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        Set<Integer> indexSet = new HashSet<>();
        for(int i=0;i<willLockNum;i++){
            //随机索引
            int index = random.nextInt(allCount);
            if(!indexSet.add(index)){
                continue;
            }
            String materialCode = ProductList.get(index);
            //随机获取商品个数
            int acquireNum = random.nextInt(3) + 1;
            UpdateResourceParam param = UpdateResourceParam.builder().resourceId(materialCode).num(acquireNum).build();
            updateResourceParamList.add(param);
        }
        //查询库存
        List<QueryResourceParam> queryResourceParamList = updateResourceParamList.stream().map(updateResourceParam -> QueryResourceParam.builder().resourceId(updateResourceParam.getResourceId()).acquire(updateResourceParam.getNum()).build()).collect(Collectors.toList());
        try {
            List<QueryResourceResult> resultList = resourceUpdater.queryAvailable(queryResourceParamList);
            for(QueryResourceResult resourceResult : resultList){
                if(!resourceResult.isFill()){
                    System.out.println("资源:" + resourceResult.getResourceId() + " 要求: " + resourceResult.getAcquire() + " 剩余: " + resourceResult.getNum() + " 不满足要求");
                    return;
                }
            }
        } catch (Exception e) {
            System.out.println("查询可用库存失败了，errMsg:" + e.getMessage());
        }

        try {
            if(useMutexLock){
                mockBizService.trySubtractResourceByMutexLock(updateResourceParamList);
            }else {
                mockBizService.trySubtractResource(new HashSet<>(updateResourceParamList));
            }
            //成功了,记录获取的资源个数，用于验证是否出现商品超卖
            updateResourceParamList.forEach(param -> {
                String resourceId = param.getResourceId();
                int permit = param.getNum();
                AtomicInteger ai = AtomicIntegerMap.get(resourceId);
                if(Objects.isNull(ai)){
                    ai = new AtomicInteger();
                    AtomicIntegerMap.putIfAbsent(resourceId,ai);
                }
                ai.addAndGet(permit);
            });
        }catch (Exception e){
            System.out.println("失败原因:" + e.getMessage());
        }
    }


    /**
     * 测试 扣减可用资源
     * 逻辑：
     * 随机选择n个商品 进行批量扣减库存（要扣减的库存数也随机生成）
     */
    private void testTryAddAvailable(){
        //随机商品个数
        int allCount = ProductList.size();
        int willLockNum = random.nextInt(allCount);
        willLockNum = willLockNum + 1;
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        Set<Integer> indexSet = new HashSet<>();
        for(int i=0;i<willLockNum;i++){
            //随机索引
            int index = random.nextInt(allCount);
            if(!indexSet.add(index)){
                continue;
            }
            String materialCode = ProductList.get(index);
            //随机获取商品个数
            int acquireNum = random.nextInt(3) + 1;
            UpdateResourceParam param = UpdateResourceParam.builder().resourceId(materialCode).num(acquireNum).build();
            updateResourceParamList.add(param);
        }
        try {
            mockBizService.tryAddAvailableResource(new HashSet<>(updateResourceParamList));

            //成功了,记录获取的资源个数，用于验证是否出现商品超卖
            updateResourceParamList.forEach(param -> {
                String resourceId = param.getResourceId();
                int permit = param.getNum();
                AtomicInteger ai = AddedAtomicIntegerMap.get(resourceId);
                if(Objects.isNull(ai)){
                    ai = new AtomicInteger();
                    AddedAtomicIntegerMap.putIfAbsent(resourceId,ai);
                }
                ai.addAndGet(permit);
            });
        }catch (Exception e){
            System.out.println("失败原因:" + e.getMessage());
        }
    }
}
