package com.ysx.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author yangshunxin
 * @create 2021-08-15-14:45
 */
public class LockTest {
    public static void main(String[] args) {
        Tick12306 tick12306 = new Tick12306();

        // 创建客户端
        Thread t1 = new Thread(tick12306, "携程");
        Thread t2 = new Thread(tick12306, "飞猪");

        t1.start();
        t2.start();

    }

}
