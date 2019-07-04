package com.kobekun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSApp {

    private static final String HDFS_PATH = "hdfs://hadoop000:8020/";

    Configuration conf = null;

    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("--------setUp---------");


        conf = new Configuration();
        conf.set("dfs.replication","1");

        /**
         * 构造一个访问指定HDFS系统的客户端对象
         * 第一个参数：HDFS的URI
         * 第二个参数：客户端指定的配置参数
         * 第三个参数：客户端的身份，说白了就是用户名
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "hadoop");
    }

    public FileSystem getInstance() throws URISyntaxException, IOException {

        System.out.println("-------------Before-------------getInstance----------");

        conf = new Configuration();

        conf.set("dfs.replication","1");

        //构造一个访问指定HDFS系统的客户端对象
//        参数1：HDFS的URI
//        参数2：客户端指定的配置参数
//        参数3：客户端身份，即客户端用户名
        fileSystem = FileSystem.get(new URI(HDFS_PATH),conf);

        return fileSystem;
    }

    /**
     * 创建HDFS文件夹
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {

        fileSystem.mkdirs(new Path("/hdfs/kobekun"));
    }

    /**
     * 读取HDFS中的内容
     * @throws IOException
     */
    @Test
    public void text() throws IOException {

        FSDataInputStream text = fileSystem.open(new Path("/kobekun.txt"));

        IOUtils.copyBytes(text,System.out,1024);
    }

    @Test
    public void createFile() throws IOException {

        FSDataOutputStream out = fileSystem
                .create(new Path("/hdfsapi/kobekun/a.txt"));

        out.writeUTF("hello kobekun: you're an excellent man!!! gogogo!!");
        out.flush();
        out.close();
    }



    @After
    public void tearDown(){

        conf = null;

        fileSystem = null;

        System.out.println("-------------tearDown----------------");
    }
}
