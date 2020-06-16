package zookepeerapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZooKeeperDemo01 {
    //确保你的Zookeeper集群必须启动
    private static final String CONNECTION="dfs01:2181,dfs02:2181,dfs03:2181";
    private static final int SESSION_TIMEOUT=3000;
    private ZooKeeper zk;
    public ZooKeeperDemo01(){
        try {
            if(zk==null){
                this.zk=new ZooKeeper(CONNECTION, SESSION_TIMEOUT, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {

                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createZnode()throws Exception{
        ArrayList<ACL> acls = new ArrayList<>();
        acls.add(new ACL(ZooDefs.Perms.ALL,new Id("world","anyone")));
        zk.create("/afei","data".getBytes(),acls, CreateMode.PERSISTENT);
    }
    public void getZnodeData()throws Exception{
        byte[] data = zk.getData("/afei", false, null);
        System.out.println(new String(data));
    }
    public void modifyZnodeData()throws Exception{
         zk.setData("/afei", "liuhai".getBytes(), -1);
    }
    public void deleteZnode()throws Exception{
        zk.delete("/data1", -1);//当前data节点没有子节点
    }
    public void rmr(String path)throws Exception{
        List<String> children = zk.getChildren(path, false);
        if(children.size()>0){
            for(String p:children){
                String newPath=path.concat("/").concat(p);
                rmr(newPath);
            }
        }
        zk.delete(path,-1);
    }
    public void close()throws Exception{
        if(zk!=null){
            zk.close();
        }
    }
    public static void main(String[] args) throws Exception {
        ZooKeeperDemo01 demo01 = new ZooKeeperDemo01();
        //demo01.createZnode();
        //demo01.getZnodeData();
        //demo01.modifyZnodeData();
       // demo01.deleteZnode();
        demo01.rmr("/data1");
        demo01.close();

    }

}
