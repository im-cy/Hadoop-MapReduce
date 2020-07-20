package word;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
 
public class exp5_main {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://localhost:9000");
		// 所有mr的输入和输出目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "hdfs://localhost:9000/user/hadoop/exp5/step1/input/inputfile.csv");
		paths.put("Step1Output", "hdfs://localhost:9000/user/hadoop/exp5/step1/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "hdfs://localhost:9000/user/hadoop/exp5/step2/output/step2");
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "hdfs://localhost:9000/user/hadoop/exp5/step3/output/step3");
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "hdfs://localhost:9000/user/hadoop/exp5/step4/output/step4");
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "hdfs://localhost:9000/user/hadoop/exp5/step5/output/step5");
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "hdfs://localhost:9000/user/hadoop/exp5/step6/output/step6");
 
		exp5_part1.run(config, paths);
		exp5_part2.run(config, paths);
		exp5_part3.run(config, paths);
		exp5_part4.run(config, paths);
		exp5_part5.run(config, paths);
		exp5_part6.run(config, paths);
	}
	
	public static Map<String, Integer> R = new HashMap<String, Integer>();
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}