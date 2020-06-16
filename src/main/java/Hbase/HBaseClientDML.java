package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseClientDML {
    private Admin admin;
    private Connection connection;
    private HTable htable;
    public HBaseClientDML(){
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

    public void close()throws Exception{
        if (admin!=null){
            admin.close();
        }
        if (connection!=null){
            connection.close();
        }

    }
    public void loadData2Table()throws Exception{
        initTable("t2");
        //put 't2','1000','f1:data','232'
        Put rowKey = new Put(Bytes.toBytes("1000"));
        //(byte [] family, byte [] qualifier, byte [] value)
        rowKey.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("data"),Bytes.toBytes("0605"));
        htable.put(rowKey);
        //
        }

    private void createTable(String tableName)throws Exception{
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor  = new HColumnDescriptor("info");
        hColumnDescriptor.setMaxVersions(5);
        HColumnDescriptor hColumnDescriptor1  = new HColumnDescriptor("location");
        tableDescriptor.addFamily(hColumnDescriptor).addFamily(hColumnDescriptor1) ;
        admin.createTable(tableDescriptor);
    }

    private void loadData(String tableName,String family,String column)throws Exception{
        initTable(tableName);
        ArrayList<Put> puts = new ArrayList<>();
        for (int i=0;i<3;i++){
            Put put = new Put(Bytes.toBytes("100".concat(String.valueOf(i))));
            put.addColumn(Bytes.toBytes(family),
                         Bytes.toBytes(column),
                         Bytes.toBytes(String.valueOf(i)));
                         puts.add(put);
        }
        htable.put(puts);
    }

    public void exe1()throws Exception{
        String tableName="t8";

        if (admin.tableExists(TableName.valueOf(tableName))){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            createTable(tableName);
        }else {
            createTable(tableName);
        }
        if (admin.tableExists(TableName.valueOf(tableName))){
            loadData(tableName,"info","age");
        }
    }

    public void getData()throws Exception{

        String tablName ="t7";
        initTable(tablName);
        Get get = new Get(Bytes.toBytes("1000"));
        Result result = htable.get(get);
        List<Cell> cells = result.listCells();
        for (Cell c: cells) {
            // hbase(main):003:0> get 't7','100'
            //COLUMN  CELL
            // info: timestamp=1591367930750, value=shijian
            long timestamp = c.getTimestamp();
            String familyArray = new String(c.getFamilyArray());
            String rowArray = new String(c.getRowArray());
            String qualifierArray = new String(c.getQualifierArray());
            String valueArray = new String(c.getValueArray());
          /*  System.out.println(String.format("familyArray:%s\n rowArray:%s\n qualifierArray:%s\n valueArray:%s",
                    familyArray,rowArray,qualifierArray,valueArray));*/


            String family=new String(c.getFamilyArray(),c.getFamilyOffset(),c.getFamilyLength());
            String rowKey=new String(c.getFamilyArray(),c.getRowOffset(),c.getRowLength());
            String qualifier=new String(c.getValueArray(),c.getQualifierOffset(),c.getQualifierLength());
            String value=new String(c.getFamilyArray(),c.getValueOffset(),c.getValueLength());
            String format=String.format("%s\t%s:%s\ttimestamp=%d,value=%s",
                    rowKey, family, qualifier, timestamp, value);
            System.out.println(format);
        }
    }

    public void getData2()throws Exception{
        String tablName ="t7";
        initTable(tablName);
        Get get = new Get(Bytes.toBytes("1000"));
        Result result = htable.get(get);
        List<Cell> cells = result.listCells();
        for (Cell c: cells) {
            long timestamp=c.getTimestamp();
            String family = new String(CellUtil.cloneFamily(c));
            String rowkey = new String(CellUtil.cloneRow(c));
            String qualifier = new String(CellUtil.cloneQualifier(c));
            String value = new String(CellUtil.cloneValue(c));
            String format=String.format("%s\t%s:%s\t timestamp=%d,value=%s",
                    rowkey,family,qualifier,timestamp,value);
            System.out.println(format);
        }
    }

    public void getDataByFamily()throws Exception {
        //get 't1','1000',{COLUMN=>'info'} //Family
        String tablName ="t7";
        initTable(tablName);
        Get get = new Get(Bytes.toBytes("1000"));
        get.addFamily(Bytes.toBytes("info"));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("riqi"));
        Result result = htable.get(get);
        List<Cell> cells = result.listCells();
        for (Cell c: cells) {
            long timestamp=c.getTimestamp();
            String family = new String(CellUtil.cloneFamily(c));
            String rowkey = new String(CellUtil.cloneRow(c));
            String qualifier = new String(CellUtil.cloneQualifier(c));
            String value = new String(CellUtil.cloneValue(c));
            String format=String.format("%s\t%s:%s\t timestamp=%d,value=%s",
                    rowkey,family,qualifier,timestamp,value);
            System.out.println(format);
        }

    }
    public void truncateTable()throws Exception{
        admin.disableTable(TableName.valueOf("t2"));
        admin.truncateTable(TableName.valueOf("t2"),true);
    }
    private void printCell(Result result)throws Exception{
        Cell[] cells = result.rawCells();
        for(Cell c:cells){
            long timestamp=c.getTimestamp();
            String family=Bytes.toString(CellUtil.cloneFamily(c));
            String rowKey=Bytes.toString(CellUtil.cloneRow(c));
            String qualifier=Bytes.toString(CellUtil.cloneQualifier(c));
            String value=Bytes.toString(CellUtil.cloneValue(c));
            String format=String.format("%s\t%s:%s\ttimestamp=%d,value=%s",
                    rowKey, family, qualifier, timestamp, value);
            System.out.println(format);
        }
    }
    public void increment()throws Exception{
        initTable("counter");
        //incr 'counter','20200506','daily:vistors',1(步长)
        Increment increment = new Increment(Bytes.toBytes("20200506"));
        increment.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("test"), 10L);
        Result result = htable.increment(increment);
        printCell(result);
    }
    public void increment2()throws Exception{
        initTable("counter");
       /* public long incrementColumnValue(final byte [] row, final byte [] family,
        final byte [] qualifier, final long amount)*/
       initTable("counter");
        long result = htable.incrementColumnValue
                        (Bytes.toBytes("20200606"),
                        Bytes.toBytes("info"),
                        Bytes.toBytes("date"),7);
        System.out.println(result);
    }
    public void scan()throws Exception{
        initTable("t1");
        ResultScanner results = htable.getScanner(Bytes.toBytes("f2"));
        for (Result res:results){
            printCell(res);
        }
    }

    public void scan1()throws Exception{
        //hbase(main):113:0> scan 't1',{COLUMN=>'f1:info'}
        initTable("t1");
        ResultScanner results = htable.getScanner(Bytes.toBytes("f2"),Bytes.toBytes("name"));
        for(Result res:results){
            printCell(res);
        }
    }
    public void append()throws Exception{
        //append 'counter','20200506','monthly:vistors','000'
        // public Append add(byte [] family, byte [] qualifier, byte [] value) {
        initTable("counter");
        Append append  = new Append(Bytes.toBytes("20200606 "));
        append.add(Bytes.toBytes("info"),
                Bytes.toBytes("riqi"),
                Bytes.toBytes("RMB"));
        Result result = htable.append(append);
        printCell(result);
    }
        public static void main(String[] args) throws Exception {
        HBaseClientDML dml = new HBaseClientDML();
        //dml.loadData2Table();
        //dml.createTable("t8");
       // dml.loadData("t7","info","riqi");
        //dml.exe1();
        //dml.getData();
        //dml.getData2();
       //dml.getDataByFamily();
       // dml.truncateTable();
        //dml.increment();
       // dml.increment2();
        //dml.scan();
      // dml.scan1();
            //dml.append();

        dml.close();

    }
}
