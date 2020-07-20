package word;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class part5_3
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
		      String line = value.toString();
	          String[] split = line.split(" ");//0 child 1 father
	          if(split[0].equals("child")){}
	          else
	          {
	              Text ins1 = new Text(split[0]);
	              Text ins2 = new Text(split[1]+"#f");
	              Text ins3 = new Text(split[1]);
	              Text ins4 = new Text(split[0]+"#c");
	              context.write(ins1,ins2);
	              context.write(ins3,ins4);
	          }
		    }
	}
/*
 * a b : f
 * a c : c
 */
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
		      List<String> grp = new ArrayList<String>();//grandp
		      List<String> grs = new ArrayList<String>();//grands
		      for(Text ins : values)
		      {
		          String str = ins.toString();
		          String[] split = str.split("#");
		          if(split[1].equals("c"))
		          {
		              grs.add(split[0]);
		          }
		          else
		          {
		              grp.add(split[0]);
		          }
		      }
		      for(String i : grs)
		      {
		          for(String j : grp)
		          {
		              Text inss1 = new Text(i);
		              Text inss2 = new Text(j);
		              context.write(inss1,inss2);
		          }
		      }
		}
	}
	public static void main(String[] args) throws Exception
	{
		  args = new String[] {"hdfs://localhost:9000/user/hadoop/exp5_3/input","hdfs://localhost:9000/user/hadoop/exp5_3/output"}; 
		  Configuration conf = new Configuration();
		  conf.set("fs.defaultFS",  "hdfs://localhost:9000");
		  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		  if (otherArgs.length != 2)
		  {
			  System.err.println("Usage: wordcount <in> <out>");
			  System.exit(2);
		  }
		  Path path =new Path(args[1]);
		  //加载配置文件 
		  FileSystem fileSystem = path.getFileSystem(conf); 
		  //输出目录若存在则删除 
		  if (fileSystem.exists(new Path(args[1])))
		  { 
			  fileSystem.delete(new Path(args[1]),true); 
		  }
		  Job job = new Job(conf, "part5_3");
		  job.setJarByClass(part5_3.class);
		  
		  job.setMapperClass(TokenizerMapper.class);
		  
	//	  job.setCombinerClass(IntSumReducer.class);
		  
		  job.setReducerClass(IntSumReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

