package word;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class kmeans {
	public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Text>
	{
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    {
	        String line = value.toString();
	        String[] point = line.split("\t");
	        List<ArrayList<Float>> cters = helper.get_cter(context.getConfiguration().get("centerpath"));
	        int k = Integer.parseInt(context.getConfiguration().get("kpath"));
	        float minn = Float.MAX_VALUE;
	        int bestp = 0;
	        for (int i = 0; i < k; i ++)
	        {
	            float dis = 0;
	            dis = Math.abs(cters.get(i).get(0) - Float.parseFloat(point[0])) * Math.abs(cters.get(i).get(0) - Float.parseFloat(point[0]))
	            		+ Math.abs(cters.get(i).get(1) - Float.parseFloat(point[1])) * Math.abs(cters.get(i).get(1) - Float.parseFloat(point[1]));
	            if (dis < minn)
	            {
	                minn = dis;
	                bestp = i;
	            }
	        }
	        /*
	         * 第ｉ簇 + 点
	         * 这样同一个簇的点被分到一起
	         */
	        context.write(new IntWritable(bestp), new Text(value));
	    }
	}
	
	public static class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>
	{
	    public void reduce(IntWritable key, Iterable<Text> value, Context context)throws IOException, InterruptedException
	    {
	        List<ArrayList<Float>> temp = new ArrayList<ArrayList<Float>>();
	        String tmpResult = "";
	        for (Text val : value)
	        {
	            String line = val.toString();
	            String[] fields = line.split("\t");
	            List<Float> tmpList = new ArrayList<Float>();
	            for (int i = 0; i < fields.length; i ++)
	            {
	                tmpList.add(Float.parseFloat(fields[i]));
	            }
	            temp.add((ArrayList<Float>) tmpList);
	        }
	        //计算新的聚类中心
	        for (int i = 0; i < temp.get(0).size(); i ++)
	        {
	            float sum = 0;
	            
	            for (int j = 0; j < temp.size(); j ++)
	                sum += temp.get(j).get(i);
	            
	            float tmp = sum / temp.size();
	            
	            if (i == 0)
	                tmpResult += tmp;
	            else
	                tmpResult += "\t" + tmp;
	        }
	        Text result = new Text(tmpResult);
	        context.write(key, result);
	    }
	}
	
	
	public static void main(String[] args) throws Exception
	{
        int loop = 10;
		args = new String[] {"hdfs://localhost:9000/user/hadoop/exp4/input","hdfs://localhost:9000/user/hadoop/exp4/output","hdfs://localhost:9000/user/hadoop/exp4/octer","hdfs://localhost:9000/user/hadoop/exp4/output/part-r-00000","5"};
        for(int ii = 1 ; ii <= loop ; ii ++)
        {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS",  "hdfs://localhost:9000");
            String[] otherArgs  = new GenericOptionsParser(conf, args).getRemainingArgs();
            conf.set("centerpath", otherArgs[2]);
            conf.set("kpath", otherArgs[4]);
            
            Job job = new Job(conf, "kmeans");
            job.setJarByClass(kmeans.class);

            Path in = new Path(otherArgs[0]);
            Path out = new Path(otherArgs[1]);
            FileSystem fs = out.getFileSystem(conf);
			fs.delete(out,true);
            fs.close();
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);

            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(true);
            helper.swapfile(args[2], args[3]);
        }
        
		Configuration conf = new Configuration();
		String[] otherArgs  = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("centerpath", otherArgs[2]);
		conf.set("kpath", otherArgs[4]);
		Job job = new Job(conf, "kmeans");
		job.setJarByClass(kmeans.class);
		Path in = new Path(otherArgs[0]);
		Path out = new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job, in);
		FileSystem fs = out.getFileSystem(conf);
		fs.delete(out,true);
		fs.close();
		FileOutputFormat.setOutputPath(job, out);
		job.setMapperClass(KmeansMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
    }
}// i x y
