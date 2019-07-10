package com.kobekun.hadoop.mapreduce.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义mapper类
 */
public class AccessMapper extends Mapper<LongWritable,Text, Text,Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        String phone = fields[1];
        long up = Long.parseLong(fields[fields.length - 3]);
        long down = Long.parseLong(fields[fields.length-2]);

        context.write(new Text(phone), new Access(phone,up,down));
    }
}
