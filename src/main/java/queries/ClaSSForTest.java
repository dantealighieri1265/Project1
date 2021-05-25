package queries;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import utils.HdfsUtility;

public class ClaSSForTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		
		Dataset<Row> provaParquet = spark.read().parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/totale-popolazione.parquet");
		JavaRDD<Row> rawParquet = provaParquet.toJavaRDD().cache();
		List<Row> prova =  rawParquet.take(100);
        for (Row l:prova) {
			System.out.println(l);
		}
        spark.close();

	}

}
