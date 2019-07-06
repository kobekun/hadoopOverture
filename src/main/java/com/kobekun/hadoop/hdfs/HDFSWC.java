package com.kobekun.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 使用HDFS API完成wordcount词频统计，并将结果输出到hdfs上
 *
 * 1、读取hdfs上的文件 ==> hdfs api
 * 2、业务处理(词频统计)：对业务中每一行都进行处理 ==> mapper
 * 3、将处理结果缓存 ==> context
 * 4、将结果输出到hdfs ==> hdfs api
 *
 */
public class HDFSWC {

    public static void main(String[] args) throws Exception {

//        1、读取文件
        Path input = new Path("/hdfsapi/kobekun/hello.txt");

        URI uri = new URI("hdfs://192.168.137.2:8020");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(uri, conf, "hadoop");

        KobekunMapper mapper = new WordCountMapper();

        KobekunContext context = new KobekunContext();

        BufferedReader reader = null;

        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(input,
                false);


        while(fileIterator.hasNext()){

            LocatedFileStatus file = fileIterator.next();

            FSDataInputStream inputStream = fs.open(file.getPath());

            String line = "";

            reader = new BufferedReader(new InputStreamReader(inputStream));

            while ((line = reader.readLine()) != null){

                // 2、进行词频统计
                mapper.map(line, context);

            }
        }

        // 3、将结果缓存起来

        Map<Object,Object> cachMap = context.getCachMap();


//
//        4、将结果输出到hdfs上
        Path output = new Path("/hdfsapi/output");

        FSDataOutputStream out = fs.create(new Path(output,
                new Path("wc.kobekun.out")));

        // 5、将步骤三缓存中的内容输出到out中
        Set<Map.Entry<Object,Object>> entries = cachMap.entrySet();

        for(Map.Entry<Object,Object> entry : entries){

            out.write((entry.getKey().toString() + "\t" +
                    entry.getValue().toString() + "\n").getBytes());
        }

        out.close();
        reader.close();

        System.out.println("词频统计完成。");
    }
}
