package utils;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.protobuf.ServiceException;

public class HBaseQueries {
	private static final String SEP = "/";
	private static final String QUERY1_TABLE = "Query1_table";
	private static final String QUERY2_TABLE = "Query2_table";
	private static final String QUERY3_TABLE_RESULTS = "Query3_table_results";
	private static final String QUERY3_TABLE_PERFORMANCE = "Query3_table_performance";
	private static final String QUERY3_TABLE_CLUSTER = "Query3_table_clusteer";
	
	private static final String COL_FAM_1 = "cf1";
	private static final String COL_FAM_2 = "cf2";
	private static final String COL_FAM_3_RESULTS = "cf3_results";
	private static final String COL_FAM_3_PERFORMANCE = "cf3_performance";
	private static final String COL_FAM_3_CLUSTER = "cf3_cluster";
	
	
	private static final String MEDIA_VACCINATI = "media_vaccinati";
	
	private static final String PREDIZIONE_VACCINATI = "predizione_vaccinati";
	
	private static final String PREDIZIONE_PERCENTUALE_VACCINATI_TOTALE = "predizione_vaccinati_totale";
	private static final String COST = "cost";
	private static final String PERFORMANCE = "performance_ms";
	private static final String CLUSTER = "cluster";
	
	
	
	HBaseClient hbc = new HBaseClient();
	SparkSession spark = SparkSession
            .builder()
            .appName("Hbase")
            .config("spark.master", "local")
            .getOrCreate();
	
	
	public void createTable(String tableName, String... columnFamilies) {
		try {
			if (hbc.exists(tableName)) {
				TableName tn = TableName.valueOf(tableName);
				hbc.getConnection().getAdmin().disableTable(tn);
				hbc.getConnection().getAdmin().deleteTable(tn);
			}
			hbc.createTable(tableName, columnFamilies);
		} catch (IOException | ServiceException e) {
			e.printStackTrace();
		}
	}
	
	public void query1_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query1_results.parquet");
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        rawResults.map(row -> {
        	hbc.put(QUERY1_TABLE, row.getString(0)+SEP+row.getString(1), COL_FAM_1, MEDIA_VACCINATI, String.valueOf(row.getInt(2)));
        	return null;
        });
	}
	public void query2_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query2_results.parquet");
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        rawResults.map(row -> {
        	hbc.put(QUERY2_TABLE, row.getString(0)+SEP+row.getString(1), COL_FAM_2, PREDIZIONE_VACCINATI, String.valueOf(row.getInt(2)));
        	return null;
        });
	}
	
	public void query3_hbase() {
		Dataset<Row> dsResults = HdfsUtility.read(spark, "query3_results.parquet");
        JavaRDD<Row> rawResults = dsResults.toJavaRDD();
        
        rawResults.map(row -> {
        	hbc.put(QUERY3_TABLE_RESULTS, row.getString(0)+SEP+row.getString(1), COL_FAM_3_RESULTS, 
        			PREDIZIONE_PERCENTUALE_VACCINATI_TOTALE, String.valueOf(row.getInt(2)));
        	return null;
        });
        
        
	}

	public static void main(String[] args) {
		HBaseClient hbc = new HBaseClient();
		//
		try {
			if (hbc.exists("query1")) {
				String name = "query1";
				TableName tableName = TableName.valueOf(name);
				hbc.getConnection().getAdmin().disableTable(tableName);
				hbc.getConnection().getAdmin().deleteTable(tableName);
			}
			
			hbc.createTable("query1", "query1_family");
			hbc.put("query1", "row2", "query1_family", "mese", "gennaio", "query1_family", "area", "Basilicata", "query1_family", "num_vacc", "16");
			hbc.put("query1", "row1", "query1_family", "mese", "gennaio", "query1_family", "area", "Basilicata", "query1_family", "num_vacc", "15");
		} catch (IOException | ServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(hbc.get("query1", "row2", "query1_family", "num_vacc"));
		System.out.println(hbc.describeTable("query1"));
		
		return;

	}

}
