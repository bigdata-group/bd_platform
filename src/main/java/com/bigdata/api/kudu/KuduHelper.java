package com.bigdata.api.kudu;

import com.bigdata.api.kudu.util.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jiwenlong on 2017/5/3.
 */
public class KuduHelper {
    private KuduClient client = null;
    private KuduSession session = null;
    public KuduHelper(){
        client = KuduUtil.getDefaultClient();
        session = client.newSession();
    }

    public KuduHelper(String kuduMasterAddr){
        client = KuduUtil.getClient(kuduMasterAddr);
        session = client.newSession();
    }

    public KuduTable createTable (String tableName, String[] fieldsNames, Type[] types, int partionNum) throws KuduException {
        if(client.tableExists(tableName))
            return client.openTable(tableName);

        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        List<String> rangeKeys = new ArrayList<String>(); // Primary key
        rangeKeys.add(fieldsNames[0]);

        for (int i = 0; i < fieldsNames.length; i++){
            ColumnSchema col;
            String colName = fieldsNames[i];
            Type colType = types[i];

            if (i == 0) {
                col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).key(true).build();
                columns.add(0, col);//To create the table, the key must be the first in the column list otherwise it will give a failure
            } else {
                col = new ColumnSchema.ColumnSchemaBuilder(colName, colType).build();
                columns.add(col);
            }
        }
        Schema schema = new Schema(columns);

        if(!client.tableExists(tableName))
            client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, partionNum));

        return client.openTable(tableName);
    }


    public void deleteTable (String tableName) throws KuduException{
        if(client.tableExists(tableName)) {
            client.deleteTable(tableName);
        }
    }

    public void setValueOfDataType(Operation oper, Common.DataType type, String field, Object value){
        switch (type) {
            case STRING:
                oper.getRow().addString(field, (String) value);
                break;
            case BINARY:
                oper.getRow().addBinary(field, (byte[]) value);
                break;
            case BOOL:
                oper.getRow().addBoolean(field, (Boolean) value);
                break;
            case INT8:
            case INT16:
            case INT32:
                oper.getRow().addInt(field, (Integer) value);
                break;
            case INT64:
                oper.getRow().addLong(field, (Long) value);
                break;
            case FLOAT:
                oper.getRow().addFloat(field, (Float) value);
                break;
            case DOUBLE:
                oper.getRow().addDouble(field, (Double) value);
                break;
            default:
                throw new IllegalArgumentException("The provided data type doesn't map" +
                        " to know any known one: " + type.getDescriptorForType().getFullName());
        }
    }

    public void insert(String tableName, String[] fieldNames, Type[] types, Object[] values) throws KuduException{
        KuduTable table = client.openTable(tableName);

        Insert insert = table.newInsert();
        int len = fieldNames.length;
        for (int i = 0; i < len; i++) {
            String field = fieldNames[i];
            Type type = types[i];
            setValueOfDataType(insert, type.getDataType(), field, values[i]);
        }
        session.apply(insert);
    }

    public void update(String tableName, String[] fieldNames, Type[] types, Object[] values) throws KuduException{
        KuduTable table = client.openTable(tableName);
        // 除了主键你设置了那列就更新哪列
        Update update = table.newUpdate();
        int len = fieldNames.length;
        for (int i = 0; i < len; i++) {
            String field = fieldNames[i];
            Type type = types[i];
            setValueOfDataType(update, type.getDataType(), field, values[i]);
        }
        session.apply(update);
    }

    public void delete(String tableName, String[] fieldNames, Type[] types, Object[] values) throws KuduException{
        KuduTable table = client.openTable(tableName);
        Delete delete = table.newDelete();
        int len = fieldNames.length;
        for (int i = 0; i < len; i++) {
            String field = fieldNames[i];
            Type type = types[i];
            setValueOfDataType(delete, type.getDataType(), field, values[i]);
        }
        session.apply(delete);
    }

    public void upsert(String tableName, String[] fieldNames, Type[] types, Object[] values) throws KuduException{
        KuduTable table = client.openTable(tableName);
        Upsert upsert = table.newUpsert();
        int len = fieldNames.length;
        for (int i = 0; i < len; i++) {
            String field = fieldNames[i];
            Type type = types[i];
            setValueOfDataType(upsert, type.getDataType(), field, values[i]);
        }
        session.apply(upsert);
    }

    public KuduScanner getKuduScanner(String tableName, long limit, String... columns) throws KuduException{
        KuduScanner scanner = client.newScannerBuilder( client.openTable(tableName)).setProjectedColumnNames(Arrays.asList(columns))
                .limit(limit).build();
        return  scanner;
//        while (scanner.hasMoreRows()) {
//            for (RowResult row : scanner.nextRows()) {
//                System.out.println(row.getString("username"));
//            }
//        }
    }

    public void closeSession() throws KuduException{
        if (session != null){
            session.close();
        }
    }


}
