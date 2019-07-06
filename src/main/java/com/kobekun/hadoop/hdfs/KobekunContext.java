package com.kobekun.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义上下文，即缓存接口
 */
public class KobekunContext {

    private Map<Object,Object> cachMap = new HashMap<Object, Object>();

    public Map<Object,Object> getCachMap(){

        return cachMap;
    }

    /**
     * 写数据到map中
     * @param key 单词
     * @param value 个数
     */
    public void write(Object key, Object value) {

        cachMap.put(key, value);
    }

    /**
     * 从缓存中获取值
     * @param key 单词
     * @return
     */
    public Object get(Object key){

        Object obj = cachMap.get(key);

        return obj;
    }


}
