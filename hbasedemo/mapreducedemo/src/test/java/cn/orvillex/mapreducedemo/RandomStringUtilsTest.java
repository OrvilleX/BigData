package cn.orvillex.mapreducedemo;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class RandomStringUtilsTest {

    @Test
    public void test() {
        //随机生成n位数数字
        String result = RandomStringUtils.randomNumeric(6);
        //在指定字符串中生成长度为n的随机字符串
        result = RandomStringUtils.random(6, "abcdefghijk");
        //指定从字符或数字中生成随机字符串
        System.out.println(RandomStringUtils.random(6, true, false));  
        System.out.println(RandomStringUtils.random(6, false, true));
    }
}
