package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HBaseClient {
    private Admin admin;
    private Connection connection;
    public HBaseClient(){
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","dfs01:2181,dfs02:2181,dfs03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void  createNameSpace() throws Exception{
        //create_namespace 'ns1'
        NamespaceDescriptor namespaceDescriptor
                = NamespaceDescriptor.create("ns1").build();

        admin.createNamespace(namespaceDescriptor);
    }
    public void describeNameSpace()throws Exception{
        NamespaceDescriptor descriptor = admin.getNamespaceDescriptor("ns1");
        System.out.println(descriptor.getName());
    }
    public void listNameSpace()throws Exception {
        NamespaceDescriptor[] listNamespaceDescriptors
                = admin.listNamespaceDescriptors();
        System.out.println(Arrays.toString(listNamespaceDescriptors));
    }
    public void alterNameSpace()throws Exception{
        HashMap<String,String> map=new HashMap<String,String>();
        map.put("info","xiaoling");
        //map.put("location", "GZ");
        NamespaceDescriptor namespaceDescriptor=
                NamespaceDescriptor.create("ns1").
                       addConfiguration(map). //hbase shell METHOD=>’set’
                       // removeConfiguration("info"). //hbase shell METHOD=>’unset’
                        build();
        admin.modifyNamespace(namespaceDescriptor);
    }
    public void listNameSpaceTables()throws Exception{
        TableName[] tableNames = admin.listTableNamesByNamespace("default");
        System.out.println(Arrays.toString(tableNames));
    }

    public void dropNameSpace()throws Exception{
        TableName[] tableNames = admin.listTableNamesByNamespace("ns1");
        for (TableName tableName: tableNames) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
        }
        admin.deleteNamespace("ns1");

    }
    public void close()throws Exception{
        if (admin!=null){
            admin.close();
        }
        if (connection!=null){
            connection.close();
        }

    }
    public static void main(String[] args) throws Exception {
        HBaseClient hBaseClient = new HBaseClient();
        //hBaseClient.createNameSpace();
        //hBaseClient.describeNameSpace();
        //hBaseClient.listNameSpace();
        //hBaseClient.alterNameSpace();
       // hBaseClient.listNameSpaceTables();
        hBaseClient.dropNameSpace();
        hBaseClient.close();
    }
}
