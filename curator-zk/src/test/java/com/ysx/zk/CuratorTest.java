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
public class CuratorTest {

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

    /**
     * 创建节点: create 持久 临时 顺序 数据
     *  1. 基本创建
     *  2. 创建节点，带有数据
     *  3. 设置节点类型
     *  4. 创建多级节点  /app1/p1
     *
     * */
    @Test
    public void testCreate4() throws Exception {
        // 创建多级节点 /app1/p1
        // creatingParentContainersIfNeeded: 如果父节点不存在 就创建父节点
        String path = client.create().creatingParentContainersIfNeeded().forPath("/app4/p1");
        System.out.println(path);
    }



    @Test
    public void testCreate3() throws Exception {
        // 设置节点类型
        // 默认类型：持久化
        String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        System.out.println(path);

    }


    @Test
    public void testCreate2() throws Exception {
        // 创建节点，带有数据
        // 如果创建节点，没有指定数据，则默认将当前客户端的ip作为数据存储
        String path = client.create().forPath("/app2", "hehe".getBytes());
        System.out.println(path);
    }

    @Test
    public void testCreate() throws Exception {
        // 基本创建
        // 如果创建节点，没有指定数据，则默认将当前客户端的ip作为数据存储
        String path = client.create().forPath("/app1");
        System.out.println(path);
    }

    //==========================get==========================
    /**
     * 查询节点：
     *  1. 查询数据：get
     *  2. 查询子节点：ls
     *  3. 查询节点状态信息： ls -s
     *
     * */

    @Test
    public void testGet() throws Exception {
        // 1. 查询数据：get
        byte[] datas = client.getData().forPath("/app1");
        System.out.println(new String(datas));
    }

    @Test
    public void testGet2() throws Exception {
        // 2. 查询子节点：ls
//        List<String> paths = client.getChildren().forPath("/app4");
        List<String> paths = client.getChildren().forPath("/");
        System.out.println(paths);
    }

    @Test
    public void testGet3() throws Exception {
        // 3. 查询节点状态信息： ls -s
        //
        Stat status = new Stat();
        System.out.println(status);
        client.getData().storingStatIn(status).forPath("/app1");
        System.out.println(status);
    }


    //==========================set==========================
    /**
     * 修改数据
     *  1. 修改数据
     *  2. 根据版本修改----多人修改
     *      version是通过查询出来的，目的就是为了让其他客户端或者线程不干扰我。
     *
     *
     * */
    @Test
    public void testSet() throws Exception {
        // 1. 修改数据
        client.setData().forPath("/app1", "itcast".getBytes());
    }


    @Test
    public void testSet2() throws Exception {
        // 一般通过这种方法 修改
        Stat status = new Stat();
        client.getData().storingStatIn(status).forPath("/app1");
        // 2. 根据版本修改----多人修改
        int version = status.getVersion(); // 查询出来的 3
        System.out.println(version);
        client.setData().withVersion(version).forPath("/app1", "hahhahhh".getBytes());
    }

    //==========================delete==========================
    /**
     * 删除节点：delete delteall
     *  1. 删除单个节点
     *  2. 删除带有子节点的节点
     *  3. 必须成功的删除: 为了防止网络抖动，多试几次
     *  4. 回调
     *
     *
     * */
    @Test
    public void testDelete() throws Exception {
        // 1. 删除单个节点
        client.delete().forPath("/app1");
    }

    @Test
    public void testDelete2() throws Exception {
        // 2. 删除带有子节点的节点
        client.delete().deletingChildrenIfNeeded().forPath("/app4");
    }

    @Test
    public void testDelete3() throws Exception {
        // 3. 必须成功的删除
        client.delete().guaranteed().forPath("/app2");
    }

    @Test
    public void testDelete4() throws Exception {
        // 4. 回调
        client.delete().guaranteed().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println("我被删除了");
                System.out.println(curatorEvent);
            }
        }).forPath("/app1");
    }


    @After
    public void close(){
        if (client != null){
            client.close();
        }
    }

}
