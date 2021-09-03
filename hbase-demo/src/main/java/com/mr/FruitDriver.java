package com.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author yangshunxin
 * @create 2021-08-23-19:10
 *
 * 读取
 *
 *
 */
public class FruitDriver implements Tool {

    // 定义一个Configuration
    private Configuration configuration = null;


    @Override
    public int run(String[] strings) throws Exception {
        
        // 1. 获取job对象
        Job job = Job.getInstance(configuration);

        // 2. 设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        // 3. 设置Mapper&Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 4. 设置Reducer类
        TableMapReduceUtil.initTableReducerJob(strings[1], FruitReducer.class, job);




        // 5. 设置输入参数
        FileInputFormat.setInputPaths(job, new Path(strings[0]));


        //6. 提交任务
        boolean b = job.waitForCompletion(true);

        return b?0:1;
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
        /**
         * 运行命令：
         *     yarn jar hbase-demo-1.0-SNAPSHOT.jar com.mr.FruitDriver /fruit.tsv  fruit1
         *
         * 将hdfs中根目录下的fruit.tsv，写入到hbase中的fruit1表中
         *
         * */


        try {

            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration, new FruitDriver(), args);
            System.exit(run);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
