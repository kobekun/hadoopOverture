package com.kobekun.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN：Map任务读数据的key类型，offset，是每一行数据起始位置的偏移量，Long
 * VALUEOUT：Map任务读数据的value类型，一行行数据，String
 *
 * hello world welcome
 * hello welcome
 *
 * KEYOUT：map方法自定义实现输出的key的类型，String
 * VALUROUT：map方法自定义实现输出的value的类型，Integer
 *
 * 词频统计：每个单词出现的次数  (world,1)
 *
 *
 * Long,String,String,Integer  --> java中的数据类型
 *
 * hadoop的自定义数据类型：支持序列化和反序列化
 * LongWritable,Text, Text, IntWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for(String word : words){
            //(hello,1) (world,1)
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
