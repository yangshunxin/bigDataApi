package com.mr2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yangshunxin
 * @create 2021-08-24-14:10
 */
public class Fruit2Driver implements Tool {

    // 定义配置信息
    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {

        // 1. 获取Job对象
        Job job = Job.getInstance(configuration);

        // 2. 设置主类路径
        job.setJarByClass(Fruit2Driver.class);

        // 3. 设置Mapper输出KV类型
        TableMapReduceUtil.initTableMapperJob(
//                strings[0],
                "fruit2",
                new Scan(),
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);


        // 4. 设置Reducer输出的表
        TableMapReduceUtil.initTableReducerJob(
                //strings[1],
                "fruit3",
                Fruit2Reducer.class, job);

        // 5. 提交任务
        boolean result = job.waitForCompletion(true);

        return result? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) {


        try {
            // 设置windows插件的环境变量
            System.setProperty("hadoop.home.dir", "D:\\bigData\\tool\\hadoop\\winutilsmaster\\hadoop-2.7.1"); // 不要也不影响结果

            // 加载库文件
            System.load("D:\\bigData\\tool\\hadoop\\winutilsmaster\\hadoop-2.7.1/bin/hadoop.dll");

//            Configuration configuration = new Configuration(); // 拷贝都集群 用yarn jar 运行
            Configuration configuration = HBaseConfiguration.create(); // 本地windows运行
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
