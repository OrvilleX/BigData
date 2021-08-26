package cn.orvillex.mapreducedemo;

import java.util.Arrays;

import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Test;

public class NumberUtilsTest {
    
    @Test
    public void test() {
        //从数组中选出最大值
        int val = NumberUtils.max(new int[] { 1, 2, 3, 4 });
        //判断字符串是否全是整数
        boolean is = NumberUtils.isDigits("153.4");
        //判断字符串是否是有效数字
        is = NumberUtils.isNumber("0321.1");
    }
}
