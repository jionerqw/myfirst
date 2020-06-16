import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfs {
    private FileSystem  fs;
    public hdfs() throws Exception {
        //封装客户端或者服务端的配置
        Configuration config= new Configuration();
        config.set("fs.defaultFS","hdfs://dfs01:9000");
      //  confreplig.set("dfs.ication", "1");
      //  config.set("dfs.blocksize", "64m");
        //FileSystem类访问Hadoop中的文件，基本方法是首先通过FileSystem类的get方法获取一个实例
        try {
            URI  uri = new URI("hdfs://dfs01:9000");
            fs= FileSystem.get(uri,config,"root");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
    public void upLoad() throws Exception {
        fs.copyFromLocalFile(new Path("D:\\file1"),new Path("/dir"));
        fs.copyFromLocalFile(new Path("D:\\file2"),new Path("/dir"));
        fs.copyFromLocalFile(new Path("D:\\file3"),new Path("/dir"));

    }
    public  void download() throws Exception {
        fs.copyToLocalFile(new Path("/word.txt"), new Path("D:\\tmp"));
    }
    public  void delete() throws IOException {
        fs.delete(new Path("/dir"),true);
    }
    public void mik() throws IOException {
        fs.mkdirs(new Path("/dir/innerdir1"));
        fs.mkdirs(new Path("/dir/innerdir2"));

    }


    public  void list() throws IOException {
    FileStatus[] fileStatuses=fs.listStatus(new Path("/dir"));
        for (FileStatus file:fileStatuses) {
           Path path= file.getPath();
           String s= path.getName();
           System.out.println("文件名称"+s);
        }

    }
public  void close() throws IOException {
        fs.close();
}
    public static void main(String[] args) throws Exception {
        hdfs opts=new hdfs();
        //上传
        //opts.upLoad();
        //下载
       // opts.download();
        opts.list();

       // opts.delete();

      //  opts.mik();

    }
}
