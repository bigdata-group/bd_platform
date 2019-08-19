package com.bigdata.api.impala.jdbc;

import com.bigdata.api.impala.util.ImpalaUtil;

import java.sql.*;

public class ImpalaJdbcClient {

    public static Connection getConnection(String connUrl, String driverClass){
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(connUrl,"1","1");
        }catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception{
        // 利用hive访问
//        try(Connection conn = getConnection(ImpalaUtil.CONNECTION_HIVE_URL, ImpalaUtil.JDBC_HIVE_DRIVER_NAME);Statement stmt = conn.createStatement();ResultSet rs = stmt.executeQuery("select * from my_impala_db.my_table1");) {
//            while (rs.next()) {
//                System.out.println(rs.getInt(1));
//            }
//        }


        Connection conn = getConnection(ImpalaUtil.CONNECTION_IMPALA_URL, ImpalaUtil.JDBC_IMPALA_DRIVER_NAME);
        System.out.print("111");

        // 利用impala访问
//        try(Connection conn = getConnection(ImpalaUtil.CONNECTION_IMPALA_URL, ImpalaUtil.JDBC_IMPALA_DRIVER_NAME);  PreparedStatement pstmt=  conn.prepareStatement("select * from my_table1 where id = ?");){
//            pstmt.setInt(1,1);
//            ResultSet rs = pstmt.executeQuery();
//            while (rs.next()) {
//                System.out.println(rs.getInt(1));
//            }
//        }

    }
}
