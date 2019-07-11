package com.kobekun.hadoop.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce自定义分区规则
 *
 * reduce个数：3
 *
 *  1%3  == 1
 *  2%3  == 2
 *  3%3  == 0
 */
public class AccessPartitioner extends Partitioner<Text,Access> {


    /**
     *
     * @param text  phone 手机号
     * @param access
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {

        String phone = text.toString();
        if(phone.startsWith("13")){
            return 0;
        }else if(phone.startsWith("15")){
            return 1;
        }else {
            return 2;
        }
    }
}
