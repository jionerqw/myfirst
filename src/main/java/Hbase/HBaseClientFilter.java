package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseClientFilter {
    private Admin admin;
    private Connection connection;
    private HTable htable;
    public HBaseClientFilter(){
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","dfs01:2181,dfs02:2181,dfs03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void initTable(String tableName)throws Exception{
        htable = (HTable)connection.getTable(TableName.valueOf(tableName));
    }
    private void printCell(Result result)throws Exception{
        Cell[] cells = result.rawCells();
        for(Cell c:cells){
            long timestamp=c.getTimestamp();
            String family= Bytes.toString(CellUtil.cloneFamily(c));
            String rowKey=Bytes.toString(CellUtil.cloneRow(c));
            String qualifier=Bytes.toString(CellUtil.cloneQualifier(c));
            String value=Bytes.toString(CellUtil.cloneValue(c));
            String format=String.format("%s\t%s:%s\ttimestamp=%d,value=%s",
                    rowKey, family, qualifier, timestamp, value);
            System.out.println(format);
        }
    }
    public void close()throws Exception{
        if (admin!=null){
            admin.close();
        }
        if (connection!=null){
            connection.close();
        }

    }

    public void rowFilter()throws Exception{
        initTable("t1");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("0")));
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for (Result res : results) {
            printCell(res);
        }
    }
    public void familyFilter()throws Exception{
        initTable("t1");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("f2")) );
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for (Result res : results) {
            printCell(res);
        }
    }
    public void qualifierFilter()throws Exception{
        initTable("t8");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("date")));
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for (Result res : results) {
            printCell(res);
        }
    }
    public void valueFilter()throws Exception{
        initTable("t8");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("0605")));
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for (Result res : results) {
            printCell(res);
        }
    }

    public void dependentColumnFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        DependentColumnFilter filter=new DependentColumnFilter(Bytes.toBytes("info"),
                Bytes.toBytes("name"),true);
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }
    public void dependentColumnFilter1()throws Exception{
        //create table
        TableName tableName=TableName.valueOf("t9");
        initTable("t9");
        if(!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor f1 = new HColumnDescriptor(Bytes.toBytes("info"));
            HColumnDescriptor f2 = new HColumnDescriptor(Bytes.toBytes("city"));
            HColumnDescriptor f3 = new HColumnDescriptor(Bytes.toBytes("location"));
            hTableDescriptor.addFamily(f1);
            hTableDescriptor.addFamily(f2);
            hTableDescriptor.addFamily(f3);
            admin.createTable(hTableDescriptor);
            //put data to table
            Put rowKey1 = new Put(Bytes.toBytes("1000"));
            rowKey1.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("age"),
                    Bytes.toBytes("12"));
            rowKey1.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("xiaoming"));
            Put rowKey2 = new Put(Bytes.toBytes("1001"));
            rowKey2.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("age"),
                    Bytes.toBytes("23"));
            rowKey2.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("lisi"));
            Put rowKey3 = new Put(Bytes.toBytes("1002"));
            rowKey3.addColumn(Bytes.toBytes("city"),
                    Bytes.toBytes("address"),
                    Bytes.toBytes("GZ"));
            Put rowKey4 = new Put(Bytes.toBytes("1003"));
            rowKey4.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("wangwu"));
            htable.put(rowKey1);
            htable.put(rowKey2);
            htable.put(rowKey3);
            htable.put(rowKey4);
        }
        //use filter
        dependentColumnFilter();
    }


    public static void main(String[] args) throws Exception {
        HBaseClientFilter app = new HBaseClientFilter();
        //app.rowFilter();
        //app.familyFilter();
        //app.qualifierFilter();
       // app.valueFilter();
        app.dependentColumnFilter1();
        app.close();
    }
}
