package word;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class page_rank_part1 {

    /*map过程*/
	/*
	 * 读入格式
	 * id + 空格 + id链接的网页
	 */
	public static class firstmapper extends Mapper<Object,Text,Text,Text>
    {
        private String id;
        private float pr;       
        private int count;
        private float average_pr = 0.1f;
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException
        {
            StringTokenizer str = new StringTokenizer(value.toString());//对value进行解析
            id = str.nextToken();//id为解析的第一个词，代表当前网页
            String temp = str.nextToken();
            String[] inss = temp.split(",");
            count = str.countTokens();
            String linkids ="&";//下面是输出的两类，分别有'@'和'&'区分
            for(String ins : inss)
            {
            	/*
            	 * A @PR
            	 */
            	context.write(new Text(ins),new Text("@"+average_pr));
            	linkids +=" "+ ins;
            }
            /*
             * A &B C D E ...
             */
            context.write(new Text(id), new Text(linkids));
        }       
    }
    public static class loopmapper extends Mapper<Object,Text,Text,Text>
    {        
        private String id;
        private float pr;
        private int count;
        private float average_pr;
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException
        {
            StringTokenizer str = new StringTokenizer(value.toString());//对value进行解析
            id =str.nextToken();//id为解析的第一个词，代表当前网页
            pr = Float.parseFloat(str.nextToken());//pr为解析的第二个词，转换为float类型，代表PageRank值
            count = str.countTokens();//count为剩余词的个数，代表当前网页的出链网页个数
            average_pr = pr/count;//求出当前网页对出链网页的贡献值
            String linkids ="&";//下面是输出的两类，分别有'@'和'&'区分
            while(str.hasMoreTokens())
            {
                String linkid = str.nextToken();
                context.write(new Text(linkid),new Text("@"+average_pr));//输出的是<出链网页，获得的贡献值>
                linkids +=" "+ linkid;
            }
            context.write(new Text(id), new Text(linkids));//输出的是<当前网页，所有出链网页>
        }
    }
    /*reduce过程*/
    public static class loopreduce extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            String lianjie = "";
            float pr = 0;
            /*对values中的每一个val进行分析，通过其第一个字符是'@'还是'&'进行判断
            通过这个循环，可以 求出当前网页获得的贡献值之和，也即是新的PageRank值；同时求出当前
            网页的所有出链网页 */
            for(Text val:values)
            {
                if(val.toString().substring(0,1).equals("@"))
                {
                    pr += Float.parseFloat(val.toString().substring(1));
                }
                else if(val.toString().substring(0,1).equals("&"))
                {
                    lianjie += val.toString().substring(1);
                }
            }
            pr = 0.85f*pr + 0.15f;//加入跳转因子，进行平滑处理            
            String result = pr+lianjie;
            context.write(key, new Text(result));
        }
    }
    public static class lastreduce extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            float pr = 0;
            /*对values中的每一个val进行分析，通过其第一个字符是'@'还是'&'进行判断
            通过这个循环，可以 求出当前网页获得的贡献值之和，也即是新的PageRank值；同时求出当前
            网页的所有出链网页 */
            for(Text val:values)
            {
                if(val.toString().substring(0,1).equals("@"))
                {
                    pr += Float.parseFloat(val.toString().substring(1));
                }
            }
            pr = 0.85f*pr + 0.15f;//加入跳转因子，进行平滑处理
            String result = pr+"";
            String temp = key.toString();
            temp = "(" + temp + "," + result + ")";
            Text nexkey = new Text(temp);
            context.write(nexkey, new Text(""));
        }
    }
    public static void main(String[] args) throws Exception
    {
		args = new String[] {"hdfs://localhost:9000/user/hadoop/exp3/input","hdfs://localhost:9000/user/hadoop/exp3/output0"};
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS",  "hdfs://localhost:9000");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf,"page rank");
		job.setJarByClass(page_rank_part1.class);
		job.setMapperClass(firstmapper.class);
		job.setReducerClass(loopreduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);//把System.exit()去掉
		int i;
		for(i=1;i<=8;i++)
		{      //加入for循环
            otherArgs[0] = otherArgs[1];
            otherArgs[1] = otherArgs[1] + i;
            job = new Job(conf,"page rank");
            job.setJarByClass(page_rank_part1.class);
            job.setMapperClass(loopmapper.class);
            job.setReducerClass(loopreduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            job.waitForCompletion(true);//把System.exit()去掉
        }
		otherArgs[0] = otherArgs[1];
        otherArgs[1] = otherArgs[1] + i;
        job = new Job(conf,"page rank");
        job.setJarByClass(page_rank_part1.class);
        job.setMapperClass(loopmapper.class);
        job.setReducerClass(lastreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);//把System.exit()去掉
		System.exit(job.waitForCompletion(true)?0:1);
    }
}
