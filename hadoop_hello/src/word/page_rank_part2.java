package word;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import avro.shaded.com.google.common.collect.Iterables;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class page_rank_part2 {
	public static void main(String[] args)
	{
		args = new String[] {"file://usr/local/hadoop/test/exp3/sparkver/input/DataSet.txt","file://usr/local/hadoop/test/exp3/sparkver/output/ans"};
		String input = args[0];
		String output = args[1];
		SparkConf config = new SparkConf().setAppName("PageRank_Part2").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(config);
		JavaRDD<String> lines = spark.textFile(input);
		
		/*
		 * link : id  list[id1,id2,id3...]
		 */
		JavaPairRDD<String,Iterable<String>> links = lines.mapToPair(s ->
		{
			String[] split = s.split("\t");
			return new Tuple2<>(split[0],Arrays.asList(split[1].split(",")));
		});
		
		//初始化value
		JavaPairRDD<String,Double> ranks = links.mapValues(rs -> 1.0);
		
		for(int i = 0 ; i < 10 ; i ++)
		{
			/*
			 * link join rank
			 * id list[] PR
			 */
			JavaPairRDD<String,Double> contributes = 
					links.join(ranks).values().flatMapToPair(s ->
					{
						int size = Iterables.size(s._1());
						List<Tuple2<String,Double>> ret = new ArrayList<>();
						for(String v : s._1())
						{
							ret.add(new Tuple2<>(v,s._2()/size));
						}
						return ret.iterator();
					});
			ranks = contributes.reduceByKey(Double::sum)
					.mapValues(rk -> rk * 0.85 + 0.15);
		}
		
		JavaRDD<String> ret = ranks.mapToPair(s -> new Tuple2<>(s._2(),s._1()))
				.sortByKey(false)
				.map(s -> String.format("(%s,%.10f)",s._2(),s._1()));
		ret.coalesce(1,true).saveAsTextFile(output);
		spark.stop();
	}
	
}
