package com.bigdata.api.impala.util;

/**
 * Created by jiwenlong on 2017/5/4.
 */
public class ImpalaUtil {
    // impala
    public static String JDBC_IMPALA_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";
    public static String CONNECTION_IMPALA_URL = "jdbc:impala://10.10.4.168:21050/default";//这个后边可以直接+数据库，数据库写错了就在default中找

    // hive
    public static String CONNECTION_HIVE_URL = "jdbc:hive2://172.20.14.170:21050/;auth=noSasl";//后边跟数据库还是查询default,直接表前边+模式吧
    public static String JDBC_HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

}
