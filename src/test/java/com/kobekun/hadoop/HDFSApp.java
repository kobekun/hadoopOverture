package com.kobekun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
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

    //创建的文件如果是以命令行的方法传上去的，就以创建的为主
//    如果代码中configuration的副本系数传上去的就以configuration上的副本系数为准
    @Test
    public void createFile() throws IOException {

//        FSDataOutputStream out = fileSystem
//                .create(new Path("/hdfsapi/kobekun/a.txt"));
        FSDataOutputStream out = fileSystem
                .create(new Path("/hdfsapi/kobekun/b.txt"));
        out.writeUTF("hello kobekun: you're an excellent man!!! gogogo!!");
        out.flush();
        out.close();
    }


    /**
     * 文件名更名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{

        Path srcPath = new Path("/hdfsapi/kobekun/b.txt");
        Path dstPath = new Path("/hdfsapi/kobekun/c.txt");

        fileSystem.rename(srcPath, dstPath);
    }

    /**
     * 拷贝本地文件到HDFS上
     * @throws Exception
     */
    @Test
    public void copyFromLocal() throws Exception{

        Path srcPath = new Path("C:\\Users\\mouse\\Desktop\\hello.txt");
        Path dstPath = new Path("/hdfsapi/kobekun/");

        fileSystem.copyFromLocalFile(srcPath, dstPath);
    }

    /**
     * 拷贝本地大文件到HDFS上：带进度
     * @throws Exception
     */
    @Test
    public void copyFromLocalBigFile() throws Exception{

        InputStream in = new BufferedInputStream(new FileInputStream(new File("C:\\Users\\mouse\\Desktop\\jdk-8u91-linux-x64.tar.gz")));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/kobekun/jdk.tgz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * hdfs copy文件到本地报空指针异常，本地copy到HDFS正常。略
     * @throws Exception
     */
    @Test
    public void copyToLocal() throws Exception{

        Path srcPath = new Path("/hdfsapi/kobekun/a.txt");
        Path dstPath = new Path("C:\\Users\\mouse\\Desktop");

        fileSystem.copyToLocalFile(srcPath, dstPath);
    }
    @Test
    public void testReplication(){

        //如果configuration不设置副本系数，
        //此处默认会加载hadoop-hdfs-2.6.0-cdh5.15.1下的hdfs-default.xml中的副本系数
//        <property>
//          <name>dfs.replication</name>
//          <value>3</value>
//          <description>Default block replication.
//                The actual number of replications can be specified when the file is created.
//                        The default is used if replication is not specified in create time.
//                        </description>
//        </property>
        System.out.println(conf.get("dfs.replication"));
    }

    /**
     * 列出HDFS文件夹下的所有文件或者文件夹
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{

        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/kobekun"));

        for(FileStatus file : statuses){

            String isDir = file.isDirectory() ? "文件夹" : "文件";

            String permission = file.getPermission().toString();

            short replication = file.getReplication();

            long length = file.getLen();

            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" +
                    length + "\t" + path);
        }
    }


    /**
     * 递归列出HDFS文件夹下的所有文件
     * @throws Exception
     */
    @Test
    public void listFilesRecursive() throws Exception{

        RemoteIterator<LocatedFileStatus> fileIterator =
                fileSystem.listFiles(new Path("/hdfsapi/kobekun/"),true);

        while (fileIterator.hasNext()){

            LocatedFileStatus file = fileIterator.next();

            String isDir = file.isDirectory() ? "文件夹" : "文件";

            String permission = file.getPermission().toString();

            short replication = file.getReplication();

            long length = file.getLen();

            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" +
                    length + "\t" + path);

        }

    }

    /**
     * 获取文件块的信息
     * 192.168.137.2:50010:0:134217728
     * 192.168.137.2:50010:134217728:47150214
     *
     * 47150214为文件的偏移量，不是当前块的大小，而是整个文件的大小
     * @throws Exception
     */
    @Test
    public void getFileBlockLocations() throws Exception{

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/kobekun/jdk.tgz"));

        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for(BlockLocation block : blockLocations){

            for(String name : block.getNames()){

                System.out.println(name + ":" + block.getOffset() + ":"
                        + block.getLength());
            }
        }
    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception{

        boolean result = fileSystem.delete(new Path("/hdfsapi/kobekun/jdk.tgz"),true);

        System.out.println(result);
    }
    @After
    public void tearDown(){

        conf = null;

        fileSystem = null;

        System.out.println("-------------tearDown----------------");
    }
}
