package com.kobekun.hadoop.userBehaviorLog.mrv2;

import com.kobekun.hadoop.mapreduce.join.MapJoin;
import com.kobekun.hadoop.userBehaviorLog.utils.GetPageId;
import com.kobekun.hadoop.userBehaviorLog.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ETL {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path("input/etl/");
        if(fs.exists(outputDir)){
            fs.delete(outputDir,true);
        }

        job.setJarByClass(MapJoin.class);
        job.setMapperClass(MyMappper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("input/etl/"));

        job.waitForCompletion(true);

    }

    public static class MyMappper extends Mapper<LongWritable, Text, NullWritable, Text>{

        private LogParser logParser;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String log = value.toString();
            Map<String,String> logInfo = logParser.parse(log);

            String ip = logInfo.get("ip");
            String url = logInfo.get("url");
            String sessionId = logInfo.get("sessionId");
            String time = logInfo.get("time");
            String country = logInfo.get("country") == null ? "-" : logInfo.get("country");
            String province = logInfo.get("province") == null ? "-" : logInfo.get("province");
            String city = logInfo.get("city") == null ? "-" : logInfo.get("city");
            String pageId = GetPageId.getPageId(url) == "" ? "-" : GetPageId.getPageId(url);

            StringBuilder builder = new StringBuilder();

            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(ip).append("\t");
            builder.append(url).append("\t");
            builder.append(sessionId).append("\t");
            builder.append(time).append("\t");
            builder.append(pageId);

            if(StringUtils.isNotBlank(pageId) && !pageId.equals("-")){
                System.out.println("--------" + pageId);
            }

            context.write(NullWritable.get(), new Text(builder.toString()));
        }
    }
}
