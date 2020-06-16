package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseClientFilter2 {
    private Admin admin;
    private Connection connection;
    private HTable htable;
    public HBaseClientFilter2(){
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
    public void singleColumnValueFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("g")
        );
        filter.setFilterIfMissing(false);
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }

    public void singleColumnValueExcludeFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("i")
        );
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }
    public void prefixFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes("1000"));

        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }
    public void columnPrefixFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("ag"));

        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }

    public void pageFilter()throws Exception{
        initTable("t9");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setStartRow(Bytes.toBytes("1000"));
        PageFilter filter = new PageFilter(2L);
        scan.setFilter(filter);
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
            printCell(result);
        }
    }
    public static void main(String[] args) throws Exception {
        HBaseClientFilter2 app = new HBaseClientFilter2();
       // app.singleColumnValueFilter();
      //  app.singleColumnValueExcludeFilter();
       // app.prefixFilter();
       // app.columnPrefixFilter();
        app.pageFilter();


    }
}
