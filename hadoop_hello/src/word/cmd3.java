package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class cmd3
{
	public static void ins(String arg1) throws IOException
	{
		System.out.println(arg1);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream inputStream = hdfs.open(new Path("/user/hadoop/test/test"));
        IOUtils.copyBytes(inputStream,System.out,1024);
        inputStream.close();		
	}
}