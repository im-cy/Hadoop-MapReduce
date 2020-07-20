package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class cmd5
{
	public static void ins(String arg1) throws IOException
	{
		System.out.println(arg1);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
        Path path = new Path(arg1);
        //通过hdfs的listStatus方法获取一个指定path的所有文件信息(status)，因此需要传入一个hdfs的路径，返回的是一个filStatus数组
        FileStatus[] fileStatuses = hdfs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            //判断当前迭代对象是否是目录
            boolean isDir = fileStatus.isDirectory();
            //获取当前文件的绝对路径
            String fullPath = fileStatus.getPath().toString();
            System.out.println("isDir:" + isDir + ",Path:" + fullPath);
        }
    }				
}