package com.kobekun.hadoop.hdfs;

/**
 * 自定义WC实现类
 */
public class WordCountMapper implements KobekunMapper {


    public void map(String line, KobekunContext context) {

        String[] words = line.split("\t");

        for(String word : words){

            Object value = context.get(word);
            //表示没有出现过该单词
            if(value == null){

                context.write(word, 1);
            }else {

                int v = Integer.parseInt(value.toString());
                //取出对应的单词，值加一
                context.write(word, v+1);
            }
        }
    }
}
