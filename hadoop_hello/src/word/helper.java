package word;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.*;

public class helper {
	public static List<ArrayList<Float>> get_cter(String inputpath)
	{
        List<ArrayList<Float>> result = new ArrayList<ArrayList<Float>>();
        Configuration conf = new Configuration();
        try{
            Path in = new Path(inputpath);
            FileSystem hdfs = in.getFileSystem(conf);
            FSDataInputStream input = hdfs.open(in);
            LineReader inputline = new LineReader(input, conf);
            Text line = new Text();
            ArrayList<Float>  cter = null;
            while (inputline.readLine(line) > 0)
            {
                String record = line.toString();
                cter = new ArrayList<Float>();
                String[] cters = record.split("\t");
                for (int i = 1; i < cters.length; i ++)
                {
                    cter.add(Float.parseFloat(cters[i]));
                }
                result.add(cter);
            }
            input.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }

    public static void deleteLastResult(String path)
    {
        Configuration conf = new Configuration();
        try {
            Path path_ = new Path(path);
            FileSystem hdfs = path_.getFileSystem(conf);
            hdfs.delete(path_, true);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void swapfile(String oldpath, String newpath)throws IOException
    {
		deleteLastResult(oldpath);
		Configuration conf = new Configuration();
		Path path0 = new Path(newpath);
		FileSystem hdfs=path0.getFileSystem(conf);
		hdfs.copyToLocalFile(new Path(newpath), new Path("/usr/local/hadoop/test/exp4/oldcter"));
		hdfs.delete(new Path(oldpath), true);
		hdfs.moveFromLocalFile(new Path("/usr/local/hadoop/test/exp4/oldcter"), new Path(oldpath));
    }
}
