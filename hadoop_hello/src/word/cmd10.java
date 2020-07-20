package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class cmd10
{
	public static void ins(String arg1,String arg2) throws IOException
	{
		System.out.println(arg1);
		System.out.println(arg2);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		try
		{
            boolean re = hdfs.rename(new Path(arg1), new Path(arg2));
            System.out.println("rename:"+re);
        }
		catch (IOException e)
		{
            e.printStackTrace();
        } 				
	}
}