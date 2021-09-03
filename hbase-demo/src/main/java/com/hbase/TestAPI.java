package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author yangshunxin
 * @create 2021-08-18-18:50
 *
 *
 * DDL:
 *  1. 判断表是否存在
 *  2. 创建表
 *  3. 创建命名空间
 *  4. 删除表
 * DML:
 *  5. 插入数据
 *  6. 查数据（get）
 *  7. 查数据（scan）
 *  8. 删除数据
 *
 *
 */
public class TestAPI {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            // 设置windows插件的环境变量
            System.setProperty("hadoop.home.dir", "D:\\bigData\\tool\\hadoop\\winutilsmaster\\hadoop-2.7.1"); // 不要也不影响结果

            // 1. 获取配置信息
            Configuration configuration = HBaseConfiguration.create();

            // 这里不要写死，不然不用配置文件中的内容
//            configuration.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
//            configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
//            configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181");

            // 2. 获取管理员对象
            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭资源
    public static void close(){

        if (admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        if (connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    // 1. 判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

        // 3. 判断表是否存在
       return admin.tableExists(TableName.valueOf(tableName));

    }

    // 2. 创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        // 1. 判断是否存在列族信息
        if (cfs.length <= 0){
            System.out.println("请设置列族信息");
            return;
        }

        // 2. 判断表是否存在
        if (isTableExist(tableName)){
            System.out.println("表已经存在："+tableName);
            return;
        }


        // 3. 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 4. 循环添加列族信息
        for (String cf : cfs){
            // 5. 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

//            hColumnDescriptor.setMaxVersions(1); // 设置版本信息

            // 6. 添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        // 7. 创建表
        admin.createTable(hTableDescriptor);

    }


    // 3. 删除表
    public static void dropTable(String tableName) throws IOException {

        // 1. 判断表是否存在
        if (!isTableExist(tableName)){
            System.out.println(tableName+ "表不存在！！！");
        }

        // 2. 使表下线
        admin.disableTable(TableName.valueOf(tableName));

        // 3. 删除表
        admin.deleteTable(TableName.valueOf(tableName));

    }
    
    // 4.创建命名空间
    public static void createNameSpace(String ns){
        
        // 1. 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        // 2. 创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e){

            System.out.println(ns+" 已存在！");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 5. 向表插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2. 创建put对象
        // 插入多个rowkey的数据，这里创建多个 put即可
        Put put = new Put(Bytes.toBytes(rowKey));

        //3. 给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        // 同一个 rowkey 添加多个列，
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));

        //4. 插入数据
        table.put(put);

        // 5. 关闭put连接---必须关闭
        table.close();
    }


    //6. 获取数据（get）
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2. 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 2.1 指定获取的列族 ---可以不加
//        get.addFamily(Bytes.toBytes(cf));

        // 2.2 指定列族和列 ---可以不加
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));


        // 2.3 获取数据的版本数 ---可以不加
        get.setMaxVersions(5);

        //3. 获取数据
        Result result = table.get(get);

        // 4. 解析result 并打印
        for (Cell cell : result.rawCells()) {
            // 5. 打印数据
            System.out.println("column family:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", column name: "+ Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }


        //6. 关闭表连接
        table.close();


    }

    // 7. 获取数据（scan）
    public static void scanTable(String tableName) throws IOException {

        //1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2. 构建scan对象
        Scan scan = new Scan(); // 不填写就全表扫描
        // 带过滤器
//        Scan scan = new Scan(Bytes.toBytes("10001"), Bytes.toBytes("10003")); // 范围扫描

        //3. 扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        // 4. 解析resultScanner
        for (Result result : resultScanner) {

            // 5. 解析result并打印
            for (Cell cell : result.rawCells()) {
                // 5. 打印数据
                System.out.println("rowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                        " column family:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", column name: "+ Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }

        //7. 关闭资源
        table.close();

    }


    // 8. 删除数据
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        /**
         * 删除操作总结：
         *
         *  rowKey->tag:delete family 删除所有的版本（versions），如果带有时间戳，删除所有小于或等于时间戳的版本
         *
         *  rowKey + column family -> tag:delete family 也是删除多个版本（versions） ，如果带有时间戳，删除所有小于或等于时间戳的版本；（命令行不支持）
         *
         *  rowKey + Column family + column name + 加s ->tag: delete column； 删除指定列所有的版本，如果带有时间戳，删除所有小于或等于时间戳的版本
         *  rowKey + Column family + column name + 不加s ->tag: delete; 删除最新的一个数据（先获取再删除），如果嗲有时间戳，只删除这个时间戳的版本
         *
         *  综上：只用不加s的版本来删除；
         *
         * */

        //1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2. 构建删除对象
        // 不设置就默认删除rowkey
        // 数据库中删除的tag: deleteFamily, 如果是多个列族，这些列族都会大这个标签
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 2.1 设置删除的列
        // 不指定时间戳，删除所有的列数据
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 会删除最新的版本，老的版本会出来---不建议使用，不符合逻辑
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 带时间戳, 删除的是这一个时间戳的数据，如果这个时间戳的数据存在就删除这一条，
        // 如果不存在，就删除当前时间戳的数据，其他的数据还在
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), 1629186692029L);

        // 2.2 删除指定的列族
        // 可以直接删除 列族，命令行不支持
        delete.addFamily(Bytes.toBytes(cf));

        // 3. 执行删除操作
        table.delete(delete);

        //4. 关闭资源
        table.close();

    }

    public static void main(String[] args) throws IOException {

//         1. 测试表是否存在
//        System.out.println(isTableExist("stu5"));

        // 2. 创建表测试
//        createTable("stu5", "info1", "info2");
        // 带命名空间的表
//        createTable("0408:stu5", "info1", "info2");

        // 3. 删除表测试
//        dropTable("stu5");

        // 4.创建命名空间测试
//        createNameSpace("0408");

        // 5.插入数据测试
//        putData("stu5", "11111", "info1", "name", "zhangsan");
//        putData("stu5", "11112", "info1", "name", "lisi");
//        putData("stu5", "11113", "info1", "name", "wangwu");
//        putData("stu5", "11114", "info1", "name", "libai");

        // 6. 获取单行数据
//        getData("stu", "10001", "info", "name");

        // 7. 测试扫描数据
        scanTable("stu5");

        // 8. 测试删除数据
//        deleteData("stu", "10003", "info", "name");


        // 测试表是否存在
//        System.out.println(isTableExist("stu5"));

        // 关闭资源
        close();
    }
}
