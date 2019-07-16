package com.kobekun.hadoop.userBehaviorLog.mr;

import com.kobekun.hadoop.userBehaviorLog.utils.GetPageId;
import com.kobekun.hadoop.userBehaviorLog.utils.IPParser;
import com.kobekun.hadoop.userBehaviorLog.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PagePVstat {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);

        Path outputpath = new Path("output/v1/pagepvstat");
        if(fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath,true);
        }
        Job job = Job.getInstance(conf);


        job.setJarByClass(PagePVstat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("output/v1/pagepvstat"));

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable> {

        private LogParser logParser;
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String log = value.toString();
            Map<String,String> logInfo = logParser.parse(log);

            String pageId = GetPageId.getPageId(logInfo.get("url"));
            context.write(new Text(pageId), ONE);
        }
    }

    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for(LongWritable access : values){
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }
}
