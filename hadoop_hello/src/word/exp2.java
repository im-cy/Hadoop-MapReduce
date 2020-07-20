package word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.io.IOException;

public class exp2
{
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	    private static Text keyInfo = new Text(); 
	    private static final Text valueInfo = new Text("1");
	    @Override  
	    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	    {
	        String line = value.toString().trim();
	        line = line.replace('\t',' ');
	        String[] fields = line.split("\\W");
	        FileSplit fileSplit = (FileSplit) context.getInputSplit();// 得到这行数据所在的文件切片
	        String fileName = fileSplit.getPath().getName();// 根据文件切片得到文件名
	        for (String field : fields)
	        {
	        	if(field.equals("\\t"))
	        		continue;
	            keyInfo.set(field + "#" + fileName);  
	            context.write(keyInfo, valueInfo);
	        }
	    }
	}
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
	{
	    private static Text info = new Text();
	    @Override  
	    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
	    {
	        int sum = 0;
	        for (Text value : values)
	        {
	            sum += Integer.parseInt(value.toString());
	        }
	        int splitIndex = key.toString().indexOf("#");
	        info.set(key.toString().substring(splitIndex + 1) + "," + sum);
	        key.set(key.toString().substring(0, splitIndex));
	        context.write(key, info);  
	    } 
	}
	public static class FileCount implements Comparable<FileCount>
	{
	    private String filename;
	    private int count;
	    //按照总流量倒序排
	    public int compareTo(FileCount bean)
	    {
	        return bean.count<this.count?1:-1;
	    }
	    public FileCount(String filename, int count)
	    {
	        this.filename = filename;
	        this.count = count;
	    }
	    @Override
	    public String toString()
	    {
	        return filename + "," + count;
	    }
	}
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
	{
	    private static Text result = new Text();
	    @Override  
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	    {
	        String fileList = new String(); 
	        List<FileCount> FileCountList = new ArrayList<FileCount>();
	        
	        for(Text value : values)
	        {
	        	String[] temp = value.toString().split(",");
	            FileCount FileCount = new FileCount(temp[0],Integer.parseInt(temp[1]));
	            FileCountList.add(FileCount);
	        }
	        
	        int total = 0;
	        Collections.sort(FileCountList);
	        
	        for (FileCount FileCount : FileCountList)
	        {  
	            fileList += "<" + FileCount.toString() + ">;";// <filename,num>;
	            String[] temp = FileCount.toString().split(",");
	            total += Integer.parseInt(temp[1]);
	        }  
	        fileList = fileList + "<total," + Integer.toString(total) + ">.";
	        result.set(fileList);  
	        context.write(key, result);  
	    }  
	}

	public static void main(String[] args) throws Exception
	{
		  args = new String[] {"hdfs://localhost:9000/user/hadoop/exp2/input","hdfs://localhost:9000/user/hadoop/exp2/output/test"}; 
		  Configuration conf = new Configuration();
		  conf.set("fs.defaultFS",  "hdfs://localhost:9000");
		  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		  Path path =new Path(args[1]);
		  FileSystem fileSystem = path.getFileSystem(conf);
		  if (fileSystem.exists(new Path(args[1])))
		  { 
			  fileSystem.delete(new Path(args[1]),true); 
		  }
		  
		  Job job = new Job(conf, "exp2");
		  job.setJarByClass(exp2.class);
		  //mapper
		  job.setMapperClass(InvertedIndexMapper.class);
		  //combiner
		  job.setCombinerClass(InvertedIndexCombiner.class);
		  //reducer
		  job.setReducerClass(InvertedIndexReducer.class);
		  //outputclass
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  
		  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  job.waitForCompletion(true);
		  return ;
	}
}

