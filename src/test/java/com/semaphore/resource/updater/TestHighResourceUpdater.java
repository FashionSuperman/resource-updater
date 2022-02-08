package com.semaphore.resource.updater;

import com.google.common.collect.Lists;
import com.semaphore.resource.updater.base.BaseSpringTest;
import com.semaphore.resource.updater.core.UpdateResourceParam;
import com.semaphore.resource.updater.bizService.MockBizService;
import org.junit.Test;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试支持预占的资源操作
 *
 * @date 2022/1/25 10:41 AM
 */
public class TestHighResourceUpdater extends BaseSpringTest {
    private static final List<String> ProductList = Lists.newArrayList("res1","res2","res3","res4","res5","res6");
    private static final Random random = new Random();
    private static final Map<String, AtomicInteger> AtomicIntegerMap = new ConcurrentHashMap<>();

    private static final Map<String, AtomicInteger> AddedAvailableMap = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> AddedPreLockMap = new ConcurrentHashMap<>();
    /**
     * 实际扣减的库存
     * 包括两个路径
     * 减少可用增加预占 减少预占
     * 直接减少可用
     */
    private static final Map<String, AtomicInteger> UsedStockMap = new ConcurrentHashMap<>();
    /**
     * 预占的库存
     */
    private static final Map<String, AtomicInteger> PreLockedMap = new ConcurrentHashMap<>();
    /**
     * 释放的预占
     */
    private static final Map<String, AtomicInteger> LeasedPreLockedMap = new ConcurrentHashMap<>();

    @Resource
    private MockBizService mockBizService;

    static {
        for (String resKey : ProductList){
            AtomicIntegerMap.putIfAbsent(resKey,new AtomicInteger());
            UsedStockMap.putIfAbsent(resKey,new AtomicInteger());
            PreLockedMap.putIfAbsent(resKey,new AtomicInteger());
            LeasedPreLockedMap.putIfAbsent(resKey,new AtomicInteger());
            AddedAvailableMap.putIfAbsent(resKey,new AtomicInteger());
            AddedPreLockMap.putIfAbsent(resKey,new AtomicInteger());
        }
    }

    /**
     * 测试组合业务逻辑 多线程
     */
    @Test
    public void testComposeBizMultiThread() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int threadNum1 = 20;
        int threadNum2 = 0;

        CountDownLatch countDownLatch = new CountDownLatch(threadNum1 + threadNum2);
        for(int i=0 ; i<threadNum1 ;i++){
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int i=0 ; i<300 ;i++){
                        try {
                            testComposeBiz();
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

        //模拟多线程增加可用库存和预占库存
        for(int i=0 ; i<threadNum2 ;i++){
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    for(int i=0 ; i<300 ;i++){
                        try {
                            testAddStock();
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.println("添加库存线程:" + Thread.currentThread() + "完成，用时: " + (System.currentTimeMillis() - startTime)/1000 + " 秒");
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }

        countDownLatch.await();
        //全部完成，输出使用的资源数量
        UsedStockMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共消耗数量: " + entry.getValue()));
        PreLockedMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共锁定数量: " + entry.getValue()));
        LeasedPreLockedMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共释放锁定数量: " + entry.getValue()));

        AddedPreLockMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共<添加>锁定数量: " + entry.getValue()));
        AddedAvailableMap.entrySet().stream().forEach(entry -> System.out.println("商品:" + entry.getKey() + " 总共<添加>可用数量: " + entry.getValue()));
        System.out.println("总用时:" + (System.currentTimeMillis() - startTime)/1000 + "秒");
    }

    private void testAddStock() {
        //随机商品个数
        List<UpdateResourceParam> updateResourceParamList = randomProduct();

        int tempRandom = random.nextInt(10);
        if(tempRandom < 3){
            //30%概率添加预占
            try {
                mockBizService.tryAddPreLockResource(new HashSet<>(updateResourceParamList));
                //成功了,记录获取的资源个数，用于验证是否出现商品超卖
                updateResourceParamList.forEach(param -> {
                    String resourceId = param.getResourceId();
                    int permit = param.getNum();
                    AtomicInteger ai = AddedPreLockMap.get(resourceId);
                    ai.addAndGet(permit);
                });
            } catch (Exception e) {
                System.out.println("添加预占库存失败，失败原因:" + e.getMessage());
            }
        }else {
            //添加可用
            try {
                mockBizService.tryAddAvailableResource(new HashSet<>(updateResourceParamList));
                //成功了,记录获取的资源个数，用于验证是否出现商品超卖
                updateResourceParamList.forEach(param -> {
                    String resourceId = param.getResourceId();
                    int permit = param.getNum();
                    AtomicInteger ai = AddedAvailableMap.get(resourceId);
                    ai.addAndGet(permit);
                });
            }catch (Exception e){
                System.out.println("添加可用库存失败，失败原因:" + e.getMessage());
            }
        }
    }

    /**
     * 测试组合业务逻辑
     * 流程如下：
     * 随机选择商品进行扣减可用增加预占
     * 有一定概率将预占库存回滚（扣减预占增加可用）（模拟超时未支付 释放库存场景）
     * 有一定概率扣减预占（模拟支付成功 扣减预占场景）
     */
    public void testComposeBiz(){
        //随机商品个数
        List<UpdateResourceParam> updateResourceParamList = randomProduct();
        int tempRandom = random.nextInt(10);
        if(tempRandom < 3){
            //30%概率直接扣减可用
            pay(updateResourceParamList);
        }else {
            //70%的概率扣减可用并增加预占
            boolean result = createOrder(updateResourceParamList);
            if(result){
                tempRandom = random.nextInt(10);
                if(tempRandom < 1){
                    //10%的概率锁定之后不操作
//                    System.out.println("锁定之后不操作");
                }
                else if(tempRandom < 3){
                    //30%的概率回滚预占
                    cancelOrder(updateResourceParamList);
                }else {
                    //60%的概率扣减预占
                    payOrder(updateResourceParamList);
                }
            }
        }
    }

    @Test
    public void testSubtractPreLockAddAvailable(){
        List<UpdateResourceParam> updateResourceParamList = new ArrayList<>();
        UpdateResourceParam param1 = UpdateResourceParam.builder().resourceId("res1").num(3).build();
        UpdateResourceParam param2 = UpdateResourceParam.builder().resourceId("res2").num(6).build();
        UpdateResourceParam param3 = UpdateResourceParam.builder().resourceId("res3").num(9).build();
        updateResourceParamList.add(param1);
        updateResourceParamList.add(param2);
        updateResourceParamList.add(param3);
        try {
            mockBizService.tryRollBackPreLock(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
        }
    }

    private void payOrder(List<UpdateResourceParam> updateResourceParamList) {
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = updateResourceParam.getResourceId();
            AtomicInteger ai = PreLockedMap.get(resourceId);
            if(Objects.isNull(ai)){
                return;
            }
            if(ai.get() < updateResourceParam.getNum()){
                return;
            }
        }
        try {
            mockBizService.trySubtractPreLock(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
            return;
        }
        updateResourceParamList.forEach(param -> {
            String resourceId = param.getResourceId();
            int permit = param.getNum();
            AtomicInteger ai = UsedStockMap.get(resourceId);
            if(Objects.isNull(ai)){
                ai = new AtomicInteger();
                UsedStockMap.putIfAbsent(resourceId,ai);
            }
            ai.addAndGet(permit);

            ai = PreLockedMap.get(resourceId);
            ai.addAndGet(-permit);
        });
    }

    /**
     * 模拟取消订单 扣减预占 增加可用
     * @param updateResourceParamList
     */
    private void cancelOrder(List<UpdateResourceParam> updateResourceParamList) {
        for(UpdateResourceParam updateResourceParam : updateResourceParamList){
            String resourceId = updateResourceParam.getResourceId();
            AtomicInteger ai = PreLockedMap.get(resourceId);
            if(Objects.isNull(ai)){
                return;
            }
            if(ai.get() < updateResourceParam.getNum()){
                return;
            }
        }
        try {
            mockBizService.tryRollBackPreLock(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
            return;
        }
        //成功了
        updateResourceParamList.forEach(param -> {
            String resourceId = param.getResourceId();
            int permit = param.getNum();
            AtomicInteger ai = LeasedPreLockedMap.get(resourceId);
            if(Objects.isNull(ai)){
                ai = new AtomicInteger();
                LeasedPreLockedMap.putIfAbsent(resourceId,ai);
            }
            ai.addAndGet(permit);

            ai = PreLockedMap.get(resourceId);
            ai.addAndGet(-permit);
        });
    }

    /**
     * 随机产生要购买的商品和个数
     * @return
     */
    private List<UpdateResourceParam> randomProduct() {
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
        return updateResourceParamList;
    }

    /**
     * 模拟创建订单 扣减可用 增加预占
     * @param updateResourceParamList
     */
    private boolean createOrder(List<UpdateResourceParam> updateResourceParamList) {
        try {
            mockBizService.trySubtractAvailableAddPreLock(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
            return false;
        }
        //成功了,记录获取的资源个数，用于验证是否出现商品超卖
        updateResourceParamList.forEach(param -> {
            String resourceId = param.getResourceId();
            int permit = param.getNum();
            AtomicInteger ai = PreLockedMap.get(resourceId);
            if(Objects.isNull(ai)){
                ai = new AtomicInteger();
                PreLockedMap.putIfAbsent(resourceId,ai);
            }
            ai.addAndGet(permit);
        });
        return true;
    }

    /**
     * 模拟直接支付场景 直接扣减库存
     * @param updateResourceParamList
     */
    private void pay(List<UpdateResourceParam> updateResourceParamList) {
        try {
            mockBizService.trySubtractResource(new HashSet<>(updateResourceParamList));
        } catch (Exception e) {
            System.out.println("失败原因:" + e.getMessage());
            return;
        }
        //成功了,记录获取的资源个数
        updateResourceParamList.forEach(param -> {
            String resourceId = param.getResourceId();
            int permit = param.getNum();
            AtomicInteger ai = UsedStockMap.get(resourceId);
            if(Objects.isNull(ai)){
                ai = new AtomicInteger();
                UsedStockMap.putIfAbsent(resourceId,ai);
            }
            ai.addAndGet(permit);
        });
    }

    /**
     * 测试 扣减可用资源 添加预占资源
     * 逻辑：
     */
    @Test
    public void testTrySubtractAvailableAddPreLockMultiThread() throws InterruptedException {
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
                            testTrySubtractAvailableAddPreLock();
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
     * 测试 扣减可用资源并增加预占资源
     * 逻辑：
     * 随机选择n个商品 进行批量扣减库存（要扣减的库存数也随机生成）
     */
    private void testTrySubtractAvailableAddPreLock(){
        //随机商品个数
        List<UpdateResourceParam> updateResourceParamList = randomProduct();
        try {
            mockBizService.trySubtractAvailableAddPreLock(new HashSet<>(updateResourceParamList));
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
}
