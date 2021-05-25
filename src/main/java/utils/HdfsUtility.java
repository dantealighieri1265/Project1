package utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class HdfsUtility {
	
	public final static String URL_HDFS = "//localhost";
	public final static String PORT_HDFS = "9871";
	public final static String INPUT_HDFS = "/data";
	public final static String OUTPUT_HDFS = "/output";
	
	public final static String QUERY1_DIR = "/Query1_results";
	public final static String QUERY2_DIR = "/Query2_results";
	public final static String QUERY3_RESULTS_DIR = "/Query3_results";
	public final static String QUERY3_PERFORMANCE_DIR = "/Query3_performance";
	public final static String QUERY3_CLUSTER_DIR = "/Query3_cluster";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static void write(Dataset<Row> dataset, String queryResults, SaveMode mode) {
		dataset.write()
               .format("csv")
               .option("header", true)
               .mode(mode)
               .save("hdfs:"+URL_HDFS+":"+PORT_HDFS+OUTPUT_HDFS+queryResults);
	}

}
