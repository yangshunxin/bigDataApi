package com.ysx.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * @author yangshunxin
 * @create 2021-08-15-19:34
 */
public class Tick12306 implements Runnable{

    private int tickets = 10; //数据库的票数

    InterProcessLock lock;

    public Tick12306(){

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
//        // 1. 第一种方式
//        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.18.121:2108,192.168.18.122:2108,192.168.18.123:2108",
//                60 * 1000, 15 * 1000, retryPolicy);

//        client.start();

        // 2. 第二种方式
        // namespace: 以后所有操作，默认这里的根目录
//        CuratorFrameworkFactory.builder();
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("192.168.18.121:2181,192.168.18.122:2181,192.168.18.123:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .build();

        client.start();

        lock = new InterProcessMutex(client, "/lock");
    }


    @Override
    public void run() {
        while (true){
            try {
                lock.acquire(3, TimeUnit.SECONDS);
                if (tickets > 0){
                    System.out.println(Thread.currentThread() + ":"+ tickets);
                    tickets--;
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }



        }


    }
}
