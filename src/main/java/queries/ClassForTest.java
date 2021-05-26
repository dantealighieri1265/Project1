package queries;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.hadoop.hbase.spark.*;

import utils.HBaseQueries;
import utils.HdfsUtility;

public class ClassForTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		
		HBaseQueries hbq = new HBaseQueries(spark);
		hbq.createTable(HBaseQueries.QUERY1_TABLE, HBaseQueries.COL_FAM_1);
		hbq.createTable(HBaseQueries.QUERY2_TABLE, HBaseQueries.COL_FAM_2);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_RESULTS, HBaseQueries.COL_FAM_3_RESULTS);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_PERFORMANCE, HBaseQueries.COL_FAM_3_PERFORMANCE);
		hbq.createTable(HBaseQueries.QUERY3_TABLE_CLUSTER, HBaseQueries.COL_FAM_3_CLUSTER);
				
		Query1.run(spark);
		Query2.run(spark);
		Query3.run(spark);
		
		hbq.query1_hbase();
		hbq.query2_hbase();
		hbq.query3_hbase();
		
		//spark.close();
		/*Dataset<Row> provaParquet = spark.read().parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-latest.parquet");
		JavaRDD<Row> rawParquet = provaParquet.toJavaRDD().cache();
		List<Row> prova =  rawParquet.take(100);
        for (Row l:prova) {
			System.out.println(l);
		}
        
        Configuration conf = HBaseConfiguration.create();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        //JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf); //sembra che lavori solo con rdd
        
        
        spark.close();*/

	}

}
