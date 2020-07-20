package word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class wordcount
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
}

	public static void main(String[] args) throws Exception
	{
		  args = new String[] {"hdfs://localhost:9000/ex0/input","hdfs://localhost:9000/ex0/output"}; 
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
		  Job job = new Job(conf, "wordcount");
		  job.setJarByClass(wordcount.class);
		  job.setMapperClass(TokenizerMapper.class);
		  job.setCombinerClass(IntSumReducer.class);
		  job.setReducerClass(IntSumReducer.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

