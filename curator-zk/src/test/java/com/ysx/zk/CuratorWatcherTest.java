package com.ysx.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangshunxin
 * @create 2021-08-15-18:17
 */
public class CuratorWatcherTest {

    private CuratorFramework client;

    /**
     * 建立连接
     *
     * */
    @Before
    public void testConnect(){
        // 第一种方式
        //connectString – list of servers to connect to, 可以为多个（集群环境下）"192.168.18.121:2108,192.168.18.122:2108"
        //sessionTimeoutMs – session timeout 回话超时时间 单位毫秒
        //connectionTimeoutMs – connection timeout 连接超时时间，单位毫秒
        //retryPolicy – retry policy to use 重试策略
        // 重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
//        // 1. 第一种方式
//        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.18.121:2108,192.168.18.122:2108,192.168.18.123:2108",
//                60 * 1000, 15 * 1000, retryPolicy);

//        client.start();

        // 2. 第二种方式
        // namespace: 以后所有操作，默认这里的根目录
//        CuratorFrameworkFactory.builder();
        client = CuratorFrameworkFactory.builder().connectString("192.168.18.121:2181,192.168.18.122:2181,192.168.18.123:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("ysx")
                .build();

        client.start();
    }

    @After
    public void close(){
        if (client != null){
            client.close();
        }
    }


    /**
     * 演示  NodeCache：给指定一个节点注册监听器
     *
     * */
    @Test
    public void testNodeCache() throws Exception {
        // 1. 创建NodeCache对象
        final NodeCache nodeCache = new NodeCache(client, "/app1");

        // 2. 注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了");
                // 获取修改节点后的数据
                byte[] datas = nodeCache.getCurrentData().getData();

                System.out.println(new String(datas));

            }
        });

        // 3. 开启监听, 如果设置为true，则开启监听 加载缓冲数据

        nodeCache.start(true);

        // 测试保证线程一直存在
        while (true){

        }

    }

    /**
     * 演示  PathChildrenCache： 监听某个节点的所有子节点们
     *
     *
     * */
    @Test
    public void testPathChildrenCache() throws Exception {
        // 1. 创建NodeCache对象
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app2", true);

        // 2. 注册监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点变化了~");
                System.out.println(pathChildrenCacheEvent);
                // 监听子节点的数据变更，并且拿到最后的数据
                // 1. 获取类型
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                // 判断类型
                if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){

                    byte[] datas = pathChildrenCacheEvent.getData().getData();
                    System.out.println(new String(datas));

                }

            }

        });

        // 3. 开启监听, 如果设置为true，则开启监听 加载缓冲数据

        pathChildrenCache.start(true);

        // 测试保证线程一直存在
        while (true){

        }

    }


    /**
     * 演示  TreeCache： 监听某个节点和它所有子节点们
     *
     *
     * */
    @Test
    public void testTreeCache() throws Exception {
        // 1. 创建TreeCache对象
        final TreeCache treeCache = new TreeCache(client, "/");

        // 2. 注册监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("数据变化了");
                System.out.println(treeCacheEvent);
            }

        });

        // 3. 开启监听, 如果设置为true，则开启监听 加载缓冲数据

        treeCache.start();

        // 测试保证线程一直存在
        while (true){

        }

    }




}
