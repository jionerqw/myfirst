package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class ZuoYe {
    private Admin admin;
    private Connection connection;
    private HTable htable;

    public ZuoYe() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "dfs01:2181,dfs02:2181,dfs03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initTable(String tableName) throws Exception {
        htable = (HTable) connection.getTable(TableName.valueOf(tableName));
    }

    private void printCell(Result result) throws Exception {
        Cell[] cells = result.rawCells();
        for (Cell c : cells) {
            long timestamp = c.getTimestamp();
            String family = Bytes.toString(CellUtil.cloneFamily(c));
            String rowKey = Bytes.toString(CellUtil.cloneRow(c));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
            String value = Bytes.toString(CellUtil.cloneValue(c));
            String format = String.format("%s\t%s:%s\ttimestamp=%d,value=%s",
                    rowKey, family, qualifier, timestamp, value);
            System.out.println(format);
        }
    }

    public void close() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
    private void createTable(String tableName)throws Exception{
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor  = new HColumnDescriptor("attends");
        hColumnDescriptor.setMaxVersions(5);
        HColumnDescriptor hColumnDescriptor1  = new HColumnDescriptor("fans");
        tableDescriptor.addFamily(hColumnDescriptor).addFamily(hColumnDescriptor1) ;
        admin.createTable(tableDescriptor);
    }
    private void loadData(String tableName,String family,String column,String values)throws Exception{
        createTable(tableName);
        initTable(tableName);
        ArrayList<Put> puts = new ArrayList<>();
        for (int i=1;i<5;i++){
            Put put = new Put(Bytes.toBytes("00000".concat(String.valueOf(i))));
            put.addColumn(Bytes.toBytes(family),
                    Bytes.toBytes(column),
                    Bytes.toBytes(values));
            puts.add(put);
        }
        htable.put(puts);
    }
    public void all() throws Exception {
        //create table
        TableName tableName = TableName.valueOf("t11");
        Scan scan = new Scan();
        initTable("t11");
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor f1 = new HColumnDescriptor(Bytes.toBytes("attends"));
            HColumnDescriptor f2 = new HColumnDescriptor(Bytes.toBytes("fans"));

            hTableDescriptor.addFamily(f1);
            hTableDescriptor.addFamily(f2);

            admin.createTable(hTableDescriptor);
            //put data to table
            Put rowKey1 = new Put(Bytes.toBytes("000001"));
            rowKey1.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000001"));
            rowKey1.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("lihua"));
            rowKey1.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("female"));
            rowKey1.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000003"));
            rowKey1.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(""));
            rowKey1.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes(""));
            Put rowKey2 = new Put(Bytes.toBytes("000001"));
            rowKey2.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000002"));
            rowKey2.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("zhangsan"));
            rowKey2.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male"));
            rowKey2.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000015"));
            rowKey2.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(""));
            rowKey2.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male "));
            Put rowKey3 = new Put(Bytes.toBytes("000002"));
            rowKey3.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000004"));
            rowKey3.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(""));
            rowKey3.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male"));
            rowKey3.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000002"));
            rowKey3.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("wanwu"));
            rowKey3.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male "));
            Put rowKey4 = new Put(Bytes.toBytes("000003"));
            rowKey4.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000006"));
            rowKey4.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(""));
            rowKey4.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("female"));
            rowKey4.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000012"));
            rowKey4.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("lisi"));
            rowKey4.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male "));
            Put rowKey5 = new Put(Bytes.toBytes("000004"));
            rowKey5.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000003"));
            rowKey5.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(""));
            rowKey5.addColumn(Bytes.toBytes("attends"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male"));
            rowKey5.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("id"),
                    Bytes.toBytes("000023"));
            rowKey5.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("liwu"));
            rowKey3.addColumn(Bytes.toBytes("fans"),
                    Bytes.toBytes("sex"),
                    Bytes.toBytes("male "));
            htable.put(rowKey1);
            htable.put(rowKey2);
            htable.put(rowKey3);
            htable.put(rowKey4);
            htable.put(rowKey5);
            ResultScanner results = htable.getScanner(scan);
            for(Result result:results){
                printCell(result);
            }

        }


    }

    public static void main(String[] args) throws Exception {
        ZuoYe zuoYe = new ZuoYe();
       // zuoYe.all();
        zuoYe.loadData("t13","attends","id","000001");
        zuoYe.loadData("t13","attends","name","lihua");
        zuoYe.loadData("t13","attends","sex","female");

        zuoYe.close();


    }
}