package com.kobekun.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoin {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoin.class);
        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置没有reduce
        job.setNumReduceTasks(0);

        //把小文件加载进分布式缓存中
        job.addCacheFile(new URI("input/join/input/dept.txt"));
        FileInputFormat.setInputPaths(job,new Path("input/join/input/emp.txt"));

        Path outputDir = new Path("input/join/mapoutput");
        outputDir.getFileSystem(conf).delete(outputDir,true);
        FileOutputFormat.setOutputPath(job,outputDir);

        job.waitForCompletion(true);
    }

//    public class MyMapper
//    Caused by: java.lang.NoSuchMethodException: com.kobekun.hadoop.mapreduce.join.MapJoin$MyMapper.<init>()
    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{

        Map<Integer,String> cache = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            String path = context.getCacheFiles()[0].toString();
            BufferedReader reader = new BufferedReader(new FileReader(path));

            String readLine = null;

            while ((readLine = reader.readLine()) != null){
                String[] splits = readLine.split("\t");  //dept
                int deptno = Integer.parseInt(splits[0]);
                String dname = splits[1];

                cache.put(deptno,dname);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            int length = splits.length;

            StringBuilder builder = new StringBuilder();
            if(length == 8){  //emp
                String empno = splits[0];
                String ename = splits[1];
                String sal = splits[5];
                int deptno = Integer.parseInt(splits[7]);
                String dname = cache.get(deptno);
                builder.append(empno).append("\t")
                        .append(ename).append("\t")
                        .append(sal).append("\t")
                        .append(dname);
            }

            context.write(new Text(builder.toString()), NullWritable.get());
        }
    }
}
