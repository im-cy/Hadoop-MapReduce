package word;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class cmd1
{
	public static void ins(String arg1,String arg2) throws IOException
	{
		System.out.println(arg1);
		System.out.println(arg2);
		Configuration conf = new Configuration(); // 以下两句中，hdfs和local分别对应HDFS实例和本地文件系统实例
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		Path inputDir;
		Path localFile, outputDir;
		FileStatus inputFiles;
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		Scanner scan;
		String str;
		byte[] buf;
		int singleFileLines;
		int numLines, numFiles, i;
		inputDir = new Path(arg1);
		// singleFileLines = Integer.parseInt(args[3]);
		if (local.exists(inputDir)) 
		{
			inputFiles = local.getFileStatus(inputDir);
			String s = inputFiles.getPath().getName();
			outputDir = new Path(arg2 + "/" + s);
			System.out.println(outputDir);
			//System.exit(0);
			if(hdfs.exists(outputDir))
			{				
				Scanner sc = new Scanner(System.in);			
				System.out.println("Input fg or zj(覆盖or追加)");
				String iput = sc.next();
				//System.out.println(iput);
				if(iput.equals("fg"))
				{
					System.out.println("You choose fg");
					out = hdfs.create(outputDir);
					in =  local.open(inputDir);
					scan = new Scanner(in);
					while (scan.hasNext()) {
						str = scan.nextLine();
						buf = (str + "\n").getBytes();
						out.write(buf, 0, buf.length);
						//System.out.println(buf);
					}
					scan.close();
					in.close();
					out.close();
				}
				if(iput.equals("zj"))
				{					
					System.out.println("You choose zj");
					out = hdfs.append(outputDir);
					in = local.open(inputDir);
					scan = new Scanner(in);
					while (scan.hasNext()) 
					{
						str = scan.nextLine();
						buf = (str + "\n").getBytes();
						out.write(buf, 0, buf.length);
					}
					scan.close();
					in.close();
					out.close();					
				}				
			}
			else
			{
				out = hdfs.create(outputDir);
				in = local.open(inputDir);
				scan = new Scanner(in);
				while (scan.hasNext()) {
					str = scan.nextLine();
					buf = (str + "\n").getBytes();
					out.write(buf, 0, buf.length);
				}
				scan.close();
				in.close();
				out.close();
			}						
		}						
		else
		{
			System.out.println("no such file");
		}						
	}	
}