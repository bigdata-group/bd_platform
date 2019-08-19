package com.bigdata.api.hbase;

import com.bigdata.api.hbase.util.HBaseByteUtils;
import com.bigdata.api.hbase.wrapper.HBasePutWrapper;
import com.bigdata.api.hbase.wrapper.HBaseTableWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 针对某张表的数据的增 删 改 查
 * Created by jiwenlong on 2017/5/1.
 */
public class HBaseTableOperation {
    private HBaseTableWrapper tableWrapper;

    public HBaseTableOperation(Connection conn, String tableName) throws IOException{
        this.tableWrapper = new HBaseTableWrapper(conn, tableName);
    }

    // 添加数据
    public void addData(byte[] key, boolean writeToWAL, String columnFamily, String column, byte[] value) throws IOException{
        HBasePutWrapper putWrapper = new HBasePutWrapper(key, writeToWAL);
        putWrapper.addColumn(HBaseByteUtils.toBytes(columnFamily), HBaseByteUtils.toBytes(column), value);
        tableWrapper.put(putWrapper);
    }

    // 删除数据
    public void delData(byte[]... keys) throws IOException{
        List<Delete> toDelLst = new ArrayList<Delete>();
        for(byte[] key : keys) {
            Delete del = new Delete(key);
            toDelLst.add(del);
        }
        tableWrapper.delete(toDelLst);
    }

    // 查询数据
    public Scan getTableScan(byte[] keyLowerBound, byte[] keyUpperBound,
                             int cacheSize, String colFamilyName, String colName){
        Scan scan = null;
        if (keyLowerBound != null) {
            if (keyUpperBound != null) {
                scan = new Scan(keyLowerBound, keyUpperBound);
            } else {
                scan = new Scan(keyLowerBound);
            }
        } else {
            scan = new Scan();
        }

        if (cacheSize > 0) {
            scan.setCaching(cacheSize);
        }

        scan.addColumn(HBaseByteUtils.toBytes(colFamilyName), HBaseByteUtils.toBytes(colName));
        return scan;
    }


    public ResultScanner executeSourceTableScan(Scan scan) throws Exception {
        if (scan.getFilter() != null) {
            if (((FilterList) scan.getFilter()).getFilters().size() == 0) {
                scan.setFilter(null);
            }
        }
        return tableWrapper.getScanner(scan);
    }

    public byte[] getRowColumnLatestValue(Result aRow, String colFamilyName, String colName){
        byte[] result = aRow.getValue(HBaseByteUtils.toBytes(colFamilyName), HBaseByteUtils.toBytes(colName));
        return result;
    }


}
