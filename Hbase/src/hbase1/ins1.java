package hbase1;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ins1
{
    // 声明静态配置
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
    }
    //创建表
    public static void creatTable(String tableName, String[] family)throws Exception 
    {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++)
        {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName))
        {
            System.out.println("table Exists!");
            System.exit(0);
        }
        else 
        {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    // 为表添加数据（适合知道有多少列族的固定表
    public static void addData(String rowKey, String tableName,String[] column1, String[] value1, String[] column2, String[] value2)throws IOException
    {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
                                                                    // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies(); // 获取所有的列族
        for (int i = 0; i < columnFamilies.length; i++)
        {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("article"))
            { // article列族put数据
                for (int j = 0; j < column1.length; j++)
                {
                    put.add(Bytes.toBytes(familyName),Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author"))
            { // author列族put数据
                for (int j = 0; j < column2.length; j++)
                {
                    put.add(Bytes.toBytes(familyName),Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }
    //遍历查询hbase表
    public static void getResultScann(String tableName) throws IOException
    {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try
        {
            rs = table.getScanner(scan);
            for (Result r : rs) 
            {
                for (KeyValue kv : r.list()) 
                {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"+ Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"+ Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        }
        finally
        {
            rs.close();
        }
    }
    //查询表中的某一列
    public static void getResultByColumn(String tableName, String rowKey,String familyName, String columnName) throws IOException
    {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list())
        {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }
    public static void rowCount(String tableName)throws IOException 
    {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        int num = 0;
    	try {
    		Scan scan = new Scan();
    		scan.setFilter(new FirstKeyOnlyFilter());
    		ResultScanner resultScanner = table.getScanner(scan);
    		for (Result result : resultScanner)
    		{
    			num += result.size();
    		}
    	}
    	catch (IOException e) {;}
        System.out.println(num+"\n");
    }
    public static void updateTable(String tableName, String rowKey,String familyName, String columnName, String value)throws IOException 
    {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }
    //删除指定的列
    public static void deleteColumn(String tableName, String rowKey,String falilyName, String columnName) throws IOException 
    {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }
    public static void deleteAllColumn(String tableName, String rowKey)throws IOException
    {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }
    //删除表
    public static void deleteTable(String tableName) throws IOException
    {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + " is deleted!");
    }
    public static void main(String[] args) throws Exception
    {
        // 创建表
        String tableName = "blog";
        String[] family = { "article", "author" };
        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {"Head First HBase","HBase is the Hadoop database.","hello Hbase" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        System.out.println("Select command:");
		Scanner sc = new Scanner(System.in);
		while(true)
		{
			String cmd = sc.next();
			if(cmd.equals("1"))
			{
		        creatTable(tableName, family);//创建表
			}
			else if(cmd.equals("2"))//添加数据
			{
		        addData("rowkey1", "blog", column1, value1, column2, value2);
		        addData("rowkey2", "blog", column1, value1, column2, value2);
		        addData("rowkey3", "blog", column1, value1, column2, value2);
			}
			else if(cmd.equals("3"))
			{
				deleteColumn("blog", "rowkey1", "author", "nickname");//删除指定一列
			}
			else if(cmd.equals("4"))
			{
				deleteAllColumn("blog", "rowkey1");//清空表
			}
			else if(cmd.equals("5"))
			{
				deleteTable("blog");//删除表
			}
			else if(cmd.equals("6"))
			{
				getResultScann("blog");
			}
			else if(cmd.equals("7"))
			{
				rowCount("blog");
			}
			else
			{
				break;
			}
		}
    }
}