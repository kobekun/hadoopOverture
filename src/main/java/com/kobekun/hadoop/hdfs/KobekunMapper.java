package com.kobekun.hadoop.hdfs;

public interface KobekunMapper {

    //关联缓存接口的map接口
    public void map(String line, KobekunContext context);
}
