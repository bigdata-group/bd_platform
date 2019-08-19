package com.bigdata.api.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by jiwenlong on 2017/5/4.
 */
public class HiveJdbcClient {
    // jdbc 连接的就是thift server
    //hive-site.xml  attr:hive.server2.thrift.bind.host
    //public final static String hiveJDBC = "jdbc:hive2://192.168.2.129:10000/default";
    public final static String hiveJDBC = "jdbc:hive2://udh-cluster-1:10000/default";

    public static void main(String[] args) throws Exception {
        HiveJdbcClient demo = new HiveJdbcClient();
        System.out.println("测试hive程序");
        demo.testHive();
    }

    public void testHive() throws Exception {//org.apache.hadoop.hive.jdbc.HiveDriver   hive就这么写
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        // 要带上这个用户名密码，就可以执行job任务,否则hive jdbc调用job会产生异常,用户名和密码不带也能访问
        Connection conn = DriverManager.getConnection(hiveJDBC, "hive", "hive");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()) {
            for (int i = 1; i < 2; i++) {
                System.out.print(rs.getString(i) + " ");
            }
            System.out.println();
        }
        free(conn, stmt, rs);
    }

    public static synchronized void free(Connection conn, Statement st,
                                         ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {

            }
    }
}
