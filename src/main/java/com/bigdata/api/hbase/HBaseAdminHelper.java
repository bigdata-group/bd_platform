package com.bigdata.api.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by jiwenlong on 2017/4/26.
 */
public class HBaseAdminHelper {
    //Hbase的admin接口提供了一系列可以获得集群访问状态的api
    private Admin admin;
    private Connection conn;
    private HBaseTableOperation tableHelper;

    public HBaseAdminHelper( Connection conn) throws IOException{
        this.conn = conn;
        this.admin = conn.getAdmin();
    }

    public HBaseTableOperation getHBaseTableOperation(String tableName) throws IOException{
        return new HBaseTableOperation(conn, tableName);
    }

    /**
     * Hbase是否可用
     * @throws Exception
     */
    public void checkHBaseAvailable() throws ServiceException,IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(
                getClass().getClassLoader());
        try {
            org.apache.hadoop.hbase.client.HBaseAdmin
                    .checkHBaseAvailable(admin.getConfiguration());
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * 返回表列表
     * @return
     * @throws IOException
     */
    public List<String> listTableNames() throws IOException{
        HTableDescriptor[] tables = admin.listTables();
        List<String> tableNames = new ArrayList<String>();
        for (HTableDescriptor h : tables) {
            tableNames.add(h.getNameAsString());
        }
        return tableNames;
    }

    public boolean tableExists( String tableName ) throws IOException {
        return admin.tableExists( TableName.valueOf( tableName ) );
    }

    public HTableDescriptor[] listTables() throws IOException {
        return admin.listTables();
    }

    public boolean isTableDisabled( String tableName ) throws IOException {
        return admin.isTableDisabled( TableName.valueOf( tableName ) );
    }

    public boolean isTableEnabled( String tableName ) throws IOException {
        return admin.isTableEnabled( TableName.valueOf( tableName ) );
    }

    public boolean isTableAvailable( String tableName ) throws IOException {
        return admin.isTableAvailable( TableName.valueOf( tableName ) );
    }

    public HTableDescriptor getTableDescriptor( byte[] tableName ) throws IOException {
        return admin.getTableDescriptor( TableName.valueOf( tableName ) );
    }

    public void enableTable( String tableName ) throws IOException {
        admin.enableTable( TableName.valueOf( tableName ) );
    }

    public void disableTable( String tableName ) throws IOException {
        admin.disableTable( TableName.valueOf( tableName ) );
    }

    public void deleteTable( String tableName ) throws IOException {
        admin.disableTable( TableName.valueOf( tableName ) );
        admin.deleteTable( TableName.valueOf( tableName ) );
    }

    public void createTable(String tableName, String ... familyNames) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        admin.createTable( tableDescriptor );
    }

    public List<String> getTableFamiles(String tableName) throws Exception {
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf( tableName ));
        Collection<HColumnDescriptor> families = descriptor.getFamilies();
        List<String> famList = new ArrayList<String>();
        for (HColumnDescriptor h : families) {
            famList.add(h.getNameAsString());
        }
        return famList;
    }

    //别忘记调用
    public void close() throws IOException {
        admin.close();
    }

}
