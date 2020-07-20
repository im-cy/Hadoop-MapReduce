package word;

import java.util.Scanner;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class part2 {
	public static void main(String[] args) throws IOException {
		System.out.println("Select command:");
		Scanner sc = new Scanner(System.in);
		while(true)
		{
			String cmd = sc.next();
			if(cmd.equals("1"))
			{
			//	cmd1.ins("/usr/local/hadoop/test/test","/user/hadoop/test");//上传文件
			//	cmd1.ins("/usr/local/hadoop/test/exp4/points","/user/hadoop/exp4/input");
				cmd1.ins("/usr/local/hadoop/test/exp5/inputfile.csv","/user/hadoop/exp5/step1/input");
				//追加或覆盖
			}
			else if(cmd.equals("2"))
			{
				//cmd2.ins("/user/hadoop/test/test", "/usr/local/hadoop/test/test");
				cmd2.ins("/user/hadoop/exp3/output0123456789/part-r-00000", "/usr/local/hadoop/test/exp3/output/test");//下载文件
			}
			else if(cmd.equals("3"))
			{
				//cmd3.ins("/user/hadoop/test/test");
				cmd3.ins("/user/hadoop/exp5_2/output/part-r-00000");
			}
			else if(cmd.equals("4"))
			{
				cmd4.ins("/user/hadoop/test/test");
			}
			else if(cmd.equals("5"))
			{
				cmd5.ins("/user/hadoop/test");
				
			}
			else if(cmd.equals("6"))
			{
				//cmd6.ins("/user/hadoop/test/test1.txt");
				cmd6.ins("/user/hadoop/exp3/output0");
				cmd6.ins("/user/hadoop/exp3/output01");
				cmd6.ins("/user/hadoop/exp3/output012");
				cmd6.ins("/user/hadoop/exp3/output0123");
				cmd6.ins("/user/hadoop/exp3/output01234");
				cmd6.ins("/user/hadoop/exp3/output012345");
				cmd6.ins("/user/hadoop/exp3/output0123456");
				cmd6.ins("/user/hadoop/exp3/output01234567");
				cmd6.ins("/user/hadoop/exp3/output012345678");
				cmd6.ins("/user/hadoop/exp3/output0123456789");
			}
			else if(cmd.equals("7"))//创建文件夹
			{
			//	cmd7.ins("/user/hadoop/test/test2");
				cmd7.ins("/user/hadoop/exp5/step1/input");
				cmd7.ins("/user/hadoop/exp5/step1/output");
				cmd7.ins("/user/hadoop/exp5/step2/input");
				cmd7.ins("/user/hadoop/exp5/step2/output");
				cmd7.ins("/user/hadoop/exp5/step3/input");
				cmd7.ins("/user/hadoop/exp5/step3/output");
				cmd7.ins("/user/hadoop/exp5/step4/input");
				cmd7.ins("/user/hadoop/exp5/step4/output");
				cmd7.ins("/user/hadoop/exp5/step5/input");
				cmd7.ins("/user/hadoop/exp5/step5/output");
				cmd7.ins("/user/hadoop/exp5/step6/input");
				cmd7.ins("/user/hadoop/exp5/step6/output");
			}
			else if(cmd.equals("8"))
			{
				cmd8.ins("/user/hadoop/test/test", " something new \n");
			}
			else if(cmd.equals("9"))
			{
				cmd9.ins("/user/hadoop/test/test2/test1.txt");
			}
			else if(cmd.equals("10"))
			{
				cmd10.ins("/user/hadoop/test/test1.txt","/user/hadoop/test/test2/test1.txt");
				
			}
			else
			{
				break;
			}
		}
	} // end of main
} // end of resultFilter

