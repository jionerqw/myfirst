package zookeeper;
import org.apache.zookeeper.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tourb on 2020/5/8.
 */
public class AppClient {
    /*
    * 1.获取链接
    * 2.zhuce
    * 3.业务代码
    * */
    private static final String CONNECTION="dfs01:2181,dfs02:2181,dfs03:2181";
    private static final int SESSION_TIMEOUT=3000;
    private ZooKeeper zk;
    private ArrayList<String> server;//存储在线的服务器节点
    public AppClient(){
        try {
            if(server==null){server=new ArrayList<String>();}
            if(zk==null){
                this.zk=new ZooKeeper(CONNECTION, SESSION_TIMEOUT, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        //完成数据初始化操作
                        System.out.println("初始化节点...");
                        try {
                            List<String> children = zk.getChildren("/server", false);
                            server.addAll(children);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    class CustomWatcher implements Watcher{
        private ZooKeeper zk;
        private String path;
        public CustomWatcher(String path,ZooKeeper zk){
            this.path=path;
            this.zk=zk;
        }

        @Override
        public void process(WatchedEvent event) {
            try {
                //准备缓冲器
                ArrayList<String> aliveServer = new ArrayList<>();
                //监听服务器上线和下线业务代码
                List<String> children = zk.getChildren("/server", false);
                aliveServer.addAll(children);
                if(server.size()>children.size()){//节点下线
                    server.removeAll(aliveServer);
                    System.out.println(server.get(0)+"节点下线");
                }else{
                    //节点上线
                    aliveServer.removeAll(server);
                    System.out.println(aliveServer.get(0)+"节点上线");
                }
                //更新最新的节点情况
                server.clear();
                server.addAll(children);
                //重复绑定监听
                zk.getChildren(path,this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void register(String path)throws Exception{
        //如果path不存在，这个时候我们需要先创建
        if(zk.exists(path,false)==null){
            zk.create(path,"path".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        CustomWatcher watcher = new CustomWatcher(path, zk);
        zk.getChildren(path,watcher);

    }
    public void close()throws Exception{
        zk.close();
    }
    public static void main(String[] args)throws Exception{
        AppClient appClient = new AppClient();
        appClient.register("/server");
        Thread.sleep(Integer.MAX_VALUE);
        appClient.close();

    }
}
