package com.kobekun.hadoop.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoin {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceJoin.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataInfo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job,new Path("input/join/input/emp.txt"), TextInputFormat.class);
        MultipleInputs.addInputPath(job,new Path("input/join/input/dept.txt"), TextInputFormat.class);

        Path outputDir = new Path("input/join/output");
        outputDir.getFileSystem(conf).delete(outputDir,true);
        FileOutputFormat.setOutputPath(job,outputDir);

        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, DataInfo>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");

            int length = splits.length;

            StringBuilder builder = new StringBuilder();

            if(length == 3){  //dept

                String dname = splits[1];

                int deptno = Integer.parseInt(splits[0]);

                DataInfo dataInfo = new DataInfo();

                dataInfo.setFlag("d");
                dataInfo.setData(dname);

                context.write(new IntWritable(deptno),dataInfo);
            }else if(length == 8){

                String empno = splits[0];
                String ename = splits[1];
                String sal = splits[5];
                int deptno = Integer.parseInt(splits[7]);

                builder.append(empno).append("\t")
                        .append(ename).append("\t")
                        .append(sal).append("\t");

                DataInfo dataInfo = new DataInfo();

                dataInfo.setFlag("e");
                dataInfo.setData(builder.toString());

                context.write(new IntWritable(deptno),dataInfo);
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable,DataInfo,Text, NullWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<DataInfo> values, Context context) throws IOException, InterruptedException {

            List<String> emps = new ArrayList<>();
            List<String> depts = new ArrayList<>();

            for(DataInfo dataInfo : values){

                if("e".equals(dataInfo.getFlag())){ //emp
                    emps.add(dataInfo.getData());
                }
                if("d".equals(dataInfo.getFlag())){ //dept
                    depts.add(dataInfo.getData());
                }
            }

            int i,j;
            for(i=0; i<emps.size(); i++){
                for (j=0; j<depts.size(); j++){

                    Text text = new Text(emps.get(i) + "\t" + depts.get(j));
                    context.write(text, NullWritable.get());
                }
            }
        }
    }
}
