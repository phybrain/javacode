package homework3;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HbaseDemo {
    public static void main(String[] args) throws Exception {
        Admin admin = HbaseUtil.getAdmin();
//        HbaseUtil.showTable();
        String nameSpace = "phy";
        String[] fields = {"name","info","score"};
        String tbName = "phy:student";
        String rowKey = "1";
//        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
        System.out.println("建表：");
        HbaseUtil.creatTable(tbName, fields);

        System.out.println("put data：");
        String[] subColums = {"name","info:student_id","info:class","score:understanding","score:programming"};
        HbaseUtil.addRecord(tbName,rowKey,subColums,new String[]{"phy","G20210735010393","4","90","90"});

        System.out.println("列表：");
        HbaseUtil.showTable();

        System.out.println("扫描表："+tbName);
        HbaseUtil.scanTable(tbName);


    }
}
