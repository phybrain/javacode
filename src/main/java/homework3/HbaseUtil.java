package homework3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;


public class HbaseUtil {

    static Connection conn = null;

    static {

        //创建连接对象

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "47.101.204.23:2181");
//        conf.set("hbase.regionserver.info.bindAddress", "47.101.204.23");
//        conf.set("hbase.regionserver.port", "16020");

        try {
            conn = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(String tableName) throws Exception {

        TableName tbName = TableName.valueOf(tableName);
        return conn.getTable(tbName);
    }

    public static void creatTable(String tableNameString, String[] fields) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString); // 首先判断表是否存在，如果存在，先将其删除
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableName + "表已经存在，现已删除，重新建立");
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (String col : fields) {
            ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build();
            tableDescriptorBuilder.setColumnFamily(columnFamily);
        }
        admin.createTable(tableDescriptorBuilder.build());

    }

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        int length = fields.length;
        for (int i = 0; i < length; i++) {
            String col = fields[i]; // 判断列族是否有列限定符
            String[] split = col.split(":");
            if (split.length == 1) {
                put.addColumn(split[0].getBytes(), "".getBytes(), values[i].getBytes());
            } else {
                put.addColumn(split[0].getBytes(), split[1].getBytes(), values[i].getBytes());
            }
            table.put(put);
        }
        // 最后要关闭表的链接
        table.close();
    }

    public static void  scanTable(String tbName) throws Exception {
        Scan scan = new Scan();
        Table table = getTable(tbName);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            HbaseUtil.showData(result);
        }
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        String[] split = column.split(":");
        if (split.length == 1) {
            ResultScanner scanner = table.getScanner(column.getBytes());
            for (Result result : scanner) {
                // 获取所有的列限定符
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(column));
                ArrayList<String> cols = new ArrayList<String>();
                for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    cols.add(Bytes.toString(entry.getKey()));
                }
                for (String str : cols) {
                    System.out.print(str + ":" + new String(result.getValue(column.getBytes(), str.getBytes())) + " | ");
                }
                System.out.println();
            }
            // 释放扫描器
            scanner.close();
        } else {
            ResultScanner scanner = table.getScanner(split[0].getBytes(), split[1].getBytes());
            for (Result result : scanner) {
                System.out.println(new String(result.getValue(split[0].getBytes(), split[1].getBytes())));
            }
            // 释放扫描器
            scanner.close();
        }
        table.close();
    }


    public static Connection getConn() {
        return conn;
    }

    public static void showData(Result result) {
        while (result.advance()) {
            Cell cell = result.current();
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String val = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(row + "--->" + cf + "--->" + qualifier + "--->" + val);
        }
    }
    public static void showTable() throws IOException {

        TableName[] tableNames = conn.getAdmin().listTableNames();
        for (TableName tableName:tableNames){
            System.out.println(tableName.getNameAsString());
        }

    }
    public static Admin getAdmin() throws Exception {
        return conn.getAdmin();
    }
}
