package com.kobekun.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用MR统计本地的文件对应的词频
 *
 * 通过combiner在map端做聚合操作减少在网络上的传输数量,提前扮演reducer的角色
 *
 */
public class WordCountCombinerLocalApp {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //创建任务
        Job job = Job.getInstance(conf);

        //设置Job对应的参数：主类
        job.setJarByClass(WordCountCombinerLocalApp.class);

        //设置job对应的mapper和reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Job对应的参数：Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Job对应的参数：Combiner的输出key和value的类型
        job.setCombinerClass(WordCountReducer.class);

        //设置Job对应的参数：reducer的输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出目录已经存在则先删除
//        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.137.2:8020"),conf,"hadoop");
//        Path output = new Path("/wordcount/kobekun/output");
//        if(fs.exists(output)){
//            fs.delete(output, true);
//        }

        //设置job的参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("input/wc.input"));
        FileOutputFormat.setOutputPath(job,new Path("output"));

        //提交作业
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}
