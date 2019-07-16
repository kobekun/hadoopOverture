package com.kobekun.hadoop.userBehaviorLog.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取页面Id
 */
public class GetPageId {

    public static String getPageId(String url){

        String pageId = "-";
        if(StringUtils.isBlank(url)){

            return pageId;
        }

        Pattern pattern = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pattern.matcher(url);

        if(matcher.find()){
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }

    public static void main(String[] args) {

        System.out.println(getPageId("http://www.yihaodian.com/cms/view.do?topicId=14572"));
    }
}
