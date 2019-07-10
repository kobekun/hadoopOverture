package com.kobekun.hadoop.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessLocalApp {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(AccessLocalApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path("access/input"));
        FileOutputFormat.setOutputPath(job, new Path("access/output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
