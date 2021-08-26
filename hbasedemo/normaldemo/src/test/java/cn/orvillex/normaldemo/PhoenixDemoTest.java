package cn.orvillex.normaldemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.Test;

public class PhoenixDemoTest {

    /**
     * 关于Spring Boot接入可以参考文档
     * https://github.com/heibaiying/BigData-Notes/blob/master/notes/Spring+Mybtais+Phoenix%E6%95%B4%E5%90%88.md
     */

    public static Connection getConnection() {
        Connection con;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
            return con;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Test
    public void query() throws SQLException {
        String sql = "select * from db1";

        Connection con = getConnection();
        Statement stmt = con.createStatement();

        // 执行查询
        ResultSet rs = stmt.executeQuery(sql);

        // 获取元数据信息
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // 获取列名
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnLabel(i);
            sb.append(columnName + "\t");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
            System.out.println(sb.toString());
            // 查询结果
            while (rs.next()) {
                sb = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(rs.getString(i) + "\t");
                }

                if (sb.length() > 0) {
                    sb.setLength(sb.length() - 1);
                    System.out.println(sb.toString());
                }
            }
        }
        con.close();
    }

    @Test
    public void test() throws Exception {
    }
}