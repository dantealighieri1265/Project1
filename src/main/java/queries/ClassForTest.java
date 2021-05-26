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

import utils.HdfsUtility;

public class ClassForTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		
		Dataset<Row> provaParquet = spark.read().parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-latest.parquet");
		JavaRDD<Row> rawParquet = provaParquet.toJavaRDD().cache();
		List<Row> prova =  rawParquet.take(100);
        for (Row l:prova) {
			System.out.println(l);
		}
        
        Configuration conf = HBaseConfiguration.create();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        //JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf); //sembra che lavori solo con rdd
        
        
        spark.close();

	}

}
