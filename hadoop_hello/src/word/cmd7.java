package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class cmd7
{
	public static void ins(String arg1) throws IOException
	{
		System.out.println(arg1);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(arg1)))//存在则删除
		{
			 try
			 {
				 boolean re = hdfs.delete(new Path(arg1), false);
				 System.out.println("delete:"+re);
		     }
			 catch (IOException e)
			 {
				 e.printStackTrace();
		     } 
		}
		else//不存在则创建
		{
			try
			{
	            hdfs.mkdirs(new Path(arg1));
	            System.out.println("cerate success");
	        } 
			catch (IOException e)
			{
	            System.out.println("cerate error");
	            e.printStackTrace();
	        } 
		}
	}
}