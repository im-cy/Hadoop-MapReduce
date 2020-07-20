package word;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class part5_2
{
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>
	{
		private IntWritable num = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String temp = value.toString();
			num.set(Integer.parseInt(temp));
			context.write(num,new IntWritable(1));
		}
	}

	public static class IntSumReducer extends Reducer<IntWritable, IntWritable,IntWritable, IntWritable>
	{
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			for(IntWritable i : values)
			{
				context.write(key,new IntWritable(0));
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		  args = new String[] {"hdfs://localhost:9000/user/hadoop/exp5_2/input","hdfs://localhost:9000/user/hadoop/exp5_2/output"}; 
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
		  Job job = new Job(conf, "part5_2");
		  job.setJarByClass(wordcount.class);
		  job.setMapperClass(TokenizerMapper.class);
		  job.setCombinerClass(IntSumReducer.class);
		  job.setReducerClass(IntSumReducer.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(IntWritable.class);
		  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

