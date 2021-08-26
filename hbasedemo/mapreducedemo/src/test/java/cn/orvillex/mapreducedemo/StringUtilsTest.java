package cn.orvillex.mapreducedemo;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class StringUtilsTest {
    
    @Test
    public void test() {

        /**
         * 缩短到某长度，并用...结尾
         */
        String result = StringUtils.abbreviate("abcdfg", 4);

        /**
         * 判断字符串末尾是否与结果匹配，如不匹配则追加
         */
        result = StringUtils.appendIfMissing("abc", "xyz");
        result = StringUtils.appendIfMissingIgnoreCase("abcXYZ", "xyz");

        /**
         * 首字符大小写转换
         */
        result = StringUtils.capitalize("cat");
        result = StringUtils.uncapitalize("Cat");

        /**
         * 扩充字符到指定大小，并让字符剧中
         */
        result = StringUtils.center("abcd", 4);
        result = StringUtils.center("ab", 4);
        result = StringUtils.center("a", 5, "yz");

        /**
         * 去除字符串中的\n \r \r\n
         */
        result = StringUtils.chomp("abc\r\n");

        /**
         * 判断字符串中是否含有特定字符串
         */
        Boolean existed = StringUtils.contains("abc", "z");
        existed = StringUtils.containsIgnoreCase("abc", "A");

        /**
         * 统计字符串中字符出现次数
         */
        int count = StringUtils.countMatches("abcdefaa", "a");

        /**
         * 删除空白字符
         */
        result = StringUtils.deleteWhitespace(" a eed fef");

        /**
         * 判断字符串是否以指定结尾
         */
        existed = StringUtils.endsWith("abcdef", "def");
        existed = StringUtils.endsWithIgnoreCase("abcdef", "DEF");
        existed = StringUtils.endsWithAny("abced", new String[] {  "a", "ed", "ff" });

        /**
         * 检查起始字符串是否匹配
         */
        existed = StringUtils.startsWith("abcdef", "abc");
        existed = StringUtils.startsWithIgnoreCase("ABCDEF", "abc");
        existed = StringUtils.startsWithAny("abcxyz", new String[] {null, "xyz", "abc"});

        //判断两字符串是否相同
        existed = StringUtils.equals("abc", "abc");
        existed = StringUtils.equalsIgnoreCase("abc", "ABC");

        //比较字符串数组内的所有元素的字符序列，起始一致则返回一致的字符串，若无则返回""
        result = StringUtils.getCommonPrefix(new String[] {"abcde", "abxyz"});

        //正向查找字符在字符串中第一次出现的位置
        count = StringUtils.indexOf("aabaabaa", "b");
        count = StringUtils.indexOf("aabaabaa", "b", 3);
        count = StringUtils.ordinalIndexOf("aabaabaa", "a", 3);

        //反向查找字符串第一次出现的位置
        count = StringUtils.lastIndexOf("aabaabaa", 'b');
        count = StringUtils.lastIndexOf("aabaabaa", 'b', 4);
        count = StringUtils.lastOrdinalIndexOf("aabaabaa", "ab", 2);

        //判断字符串大写、小写
        existed = StringUtils.isAllUpperCase("ABC");
        existed = StringUtils.isAllLowerCase("abC");

        //判断是否为空(注：isBlank与isEmpty 区别)
        existed = StringUtils.isBlank(null);StringUtils.isBlank("");StringUtils.isBlank(" ");
        existed = StringUtils.isNoneBlank(" ", "bar");

        existed = StringUtils.isEmpty(null);StringUtils.isEmpty("");
        existed = StringUtils.isEmpty(" ");
        existed = StringUtils.isNoneEmpty(" ", "bar");

        //判断字符串数字
        existed = StringUtils.isNumeric("123");
        existed = StringUtils.isNumeric("12 3");
        existed = StringUtils.isNumericSpace("12 3");

        //大小写转换
        result = StringUtils.upperCase("aBc");
        result = StringUtils.lowerCase("aBc");
        result = StringUtils.swapCase("The dog has a BONE");

        //替换字符串内容……（replacePattern、replceOnce）
        result = StringUtils.replace("aba", "a", "z");
        result = StringUtils.overlay("abcdef", "zz", 2, 4);
        result = StringUtils.replaceEach("abcde", new String[]{"ab", "d"},
                new String[]{"w", "t"});

        //重复字符
        result = StringUtils.repeat('e', 3);

        //反转字符串
        result = StringUtils.reverse("bat");

        //删除某字符
        result = StringUtils.remove("queued", 'u');

        //分割字符串
        String[] strs = StringUtils.split("a..b.c", '.');
        strs = StringUtils.split("ab:cd:ef", ":", 2);
        strs = StringUtils.splitByWholeSeparator("ab-!-cd-!-ef", "-!-", 2);
        strs = StringUtils.splitByWholeSeparatorPreserveAllTokens("ab::cd:ef", ":");

        //去除首尾空格，类似trim……（stripStart、stripEnd、stripAll、stripAccents）
        result = StringUtils.strip(" ab c ");
        result = StringUtils.stripToNull(null);
        result = StringUtils.stripToEmpty(null);

        //截取字符串
        result = StringUtils.substring("abcd", 2);
        result = StringUtils.substring("abcdef", 2, 4);

        //left、right从左(右)开始截取n位字符
        result = StringUtils.left("abc", 2);
        result = StringUtils.right("abc", 2);
        //从第n位开始截取m位字符       n  m
        result = StringUtils.mid("abcdefg", 2, 4);

        result = StringUtils.substringBefore("abcba", "b");
        result = StringUtils.substringBeforeLast("abcba", "b");
        result = StringUtils.substringAfter("abcba", "b");
        result = StringUtils.substringAfterLast("abcba", "b");

        result = StringUtils.substringBetween("tagabctag", "tag");
        result = StringUtils.substringBetween("yabczyabcz", "y", "z");
    }
}
