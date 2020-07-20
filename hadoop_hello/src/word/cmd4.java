package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class cmd4
{
	public static void ins(String arg1) throws IOException
	{
		System.out.println(arg1);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus fs = hdfs.getFileStatus(new Path(arg1));
		System.out.println("path: "+fs.getPath());
		System.out.println("length: "+fs.getLen());
		System.out.println("modify time: "+fs.getModificationTime());
		System.out.println("owner: "+fs.getOwner());
		System.out.println("replication: "+fs.getReplication());
		System.out.println("blockSize: "+fs.getBlockSize());
		System.out.println("group: "+fs.getGroup());
		System.out.println("permission: "+fs.getPermission().toString());				
	}
}