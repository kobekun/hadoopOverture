package com.kobekun.hadoop.userBehaviorLog.mrv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 版本一：流量统计
 */
public class PVStatV2 {


    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);

        Path outputpath = new Path("output/v2/pvstat");
        if(fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath);
        }
        Job job = Job.getInstance(conf);


        job.setJarByClass(PVStatV2.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input/etl"));
        FileOutputFormat.setOutputPath(job,new Path("output/v2/pvstat"));

        job.waitForCompletion(true);

    }


    static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable>{

        private LongWritable ONE = new LongWritable(1);

        private Text KEY = new Text("key");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(KEY, ONE);
        }

    }

    static class MyReducer extends Reducer<Text,LongWritable, NullWritable,LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (LongWritable value : values){

                count++;
            }
            context.write(NullWritable.get(),new LongWritable(count));
        }
    }
}
