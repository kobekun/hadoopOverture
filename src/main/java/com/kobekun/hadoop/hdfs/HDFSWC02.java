package com.kobekun.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * hadoop fs -ls == hadoop fs -ls /user/hadoop
 */
public class HDFSWC02 {

    public static void main(String[] args) throws Exception {

        Properties properties = GetPropertiesFromResourcesUtils.getProperties();

//        1、读取文件
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        URI uri = new URI(properties.getProperty(Constants.HDFS_URI));

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(uri, conf, "hadoop");

        //通过反射获取对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));

        KobekunMapper mapper = (KobekunMapper) clazz.newInstance();

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
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));

        FSDataOutputStream out = fs.create(new Path(output,
                properties.getProperty(Constants.OUTPUT_FILE)));

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
