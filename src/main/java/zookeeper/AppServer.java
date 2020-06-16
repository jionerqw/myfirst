package zookeeper;

import org.apache.zookeeper.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tourb on 2020/5/8.
 */
public class AppServer {
    private static final String CONNECTION="dfs01:2181,dfs02:2181,dfs03:2181";
    private static final int SESSION_TIMEOUT=3000;
    private ZooKeeper zk;
    //获取连接
    public AppServer(){
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
        //注册
    public void register(String path)throws Exception{
        if(zk.exists(path,false)==null){
            zk.create(path,"path".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        //如果指定的/server节点存在的。注册临时有序的节点
        String znode=zk.create("/server/service","service".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(znode+" :服务器已经上线");

    }
    public void close()throws Exception{
        zk.close();
    }

    public static void main(String[] args)throws Exception{
        AppServer appServer = new AppServer();
        appServer.register("/server");
        Thread.sleep(Integer.MAX_VALUE);
        appServer.close();

    }
}
