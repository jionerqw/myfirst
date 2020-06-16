package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;

public class HBaseClientD {
    private Admin admin;
    private Connection connection;
    private Configuration conf;
    public HBaseClientD(){
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","dfs01:2181,dfs02:2181,dfs03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void createTable()throws Exception{
        TableName tableName = TableName.valueOf("t5");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor family1  = new HColumnDescriptor("f1");
        HColumnDescriptor family2  = new HColumnDescriptor("f2");
        HColumnDescriptor family3  = new HColumnDescriptor("f3");
        hTableDescriptor.addFamily(family1).addFamily(family2).addFamily(family3);
        admin.createTable(hTableDescriptor);
    }
    public void createTable_1()throws Exception{
        //create 't5',{NAME=>'f1',VERSIONS=>5},{NAME=>'f2',VERSIONS=>2},{NAME=>'f3'}
        TableName tableName = TableName.valueOf("t6");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor family1  = new HColumnDescriptor("f1");
        family1.setMaxVersions(5);
        HColumnDescriptor family2  = new HColumnDescriptor("f2");
        family2.setMaxVersions(3);
        HColumnDescriptor family3  = new HColumnDescriptor("f3");
        family3.setMaxVersions(2);

        hTableDescriptor.addFamily(family1).addFamily(family2).addFamily(family3);
        admin.createTable(hTableDescriptor);
        }
    public void createTable_2()throws Exception{
        //create 't7',{NAME=>'f1',VERSIONS=>5,TTL=>10},{NAME=>'f2',VERSIONS=>2},{NAME=>'f3'}
        TableName tableName = TableName.valueOf("t7");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //add a column family
        HColumnDescriptor family1 = new HColumnDescriptor("f1");
        family1.setMaxVersions(5);
        family1.setTimeToLive(10);
        HColumnDescriptor family2 = new HColumnDescriptor("f2");
        family2.setMaxVersions(2);
        HColumnDescriptor family3 = new HColumnDescriptor("f3");
        hTableDescriptor.addFamily(family1).
                addFamily(family2).
                addFamily(family3);
        admin.createTable(hTableDescriptor);
    }

    public void close()throws Exception{
        if (admin!=null){
            admin.close();
        }
        if (connection!=null){
            connection.close();
        }
    }

    public void existsTable()throws Exception{
        //exists 't1' //true,false

        String result = admin.tableExists(TableName.valueOf("t2")) ? "存在" : "不存在";
        System.out.println(result);
    }

    public void existsTable_1()throws Exception{
        //exists 't1' //true,false
        HBaseAdmin hBaseAdmin = (HBaseAdmin)admin;
        String result= hBaseAdmin.tableExists("t2")?"存在":"不存在";
        System.out.println(result);
    }

    public void listTables()throws Exception{
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor table:hTableDescriptors) {
            System.out.println(table.toString());

        }
    }
    public void listTables_1()throws Exception{
        HTableDescriptor[] hTableDescriptors = admin.listTables("t[1-2]");
        for (HTableDescriptor table:hTableDescriptors) {
            System.out.println(table.toString());

        }

    }

    public void deleteColumn()throws Exception{

        //    void deleteColumn(TableName var1, byte[] var2) throws IOException;
        admin.deleteColumn(TableName.valueOf("t6"), Bytes.toBytes("f1"));

    }

    public void alterStatus()throws Exception{
        Pair<Integer, Integer> t1 = admin.getAlterStatus(TableName.valueOf("t1"));
        Integer first = t1.getFirst();//the regions that are yet to be updated
        Integer second = t1.getSecond();//total number of regions of the table
        System.out.println(first+"---"+second);
    }

    public void deleteTable(String tableName) throws IOException {
        
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }
    /*public  void dropTable(String tableName) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }*/

    public static void main(String[] args) throws Exception {
        HBaseClientD hBaseClientD = new HBaseClientD();
       //hBaseClientD.createTable();
       // hBaseClientD.createTable_1();
        //hBaseClientD.createTable_2();
        //hBaseClientD.existsTable();
        //hBaseClientD.listTables();
       // hBaseClientD.listTables_1();
        //hBaseClientD.deleteColumn();
       // hBaseClientD.alterStatus();
        hBaseClientD.deleteTable("t7");


    }
}
