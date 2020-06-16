package zookepeerapi;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperDemo02 {
    private static final String CONNECTION="dfs01:2181,dfs02:2181,dfs03:2181";
    private static final int SESSION_TIMEOUT=3000;
    private ZooKeeper zk;
    public ZooKeeperDemo02(){
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
    public class NodeChildrenChangedWatcher implements Watcher{
        private ZooKeeper zk;
        private String path;
        public NodeChildrenChangedWatcher(){}
        public NodeChildrenChangedWatcher(String path,ZooKeeper zk)
        {   this.path=path;
            this.zk=zk;
        }
        @Override
        public void process(WatchedEvent event) {
            //当监听的事件一旦被触发的时候，这个process()会被回调
            System.out.println(event.getType());

            //重复绑定监听
            try {
                zk.getChildren(path,this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void register(String path)throws Exception{
        NodeChildrenChangedWatcher nodeChildChangeWathcer=new NodeChildrenChangedWatcher(path,zk);
        zk.getChildren(path,nodeChildChangeWathcer);
    }
    public static void main(String[] args)throws Exception{
        ZooKeeperDemo02 zooKeeperDemo02 = new ZooKeeperDemo02();
        zooKeeperDemo02.register("/ll");//注册监听器
        Thread.sleep(Integer.MAX_VALUE);
    }
}