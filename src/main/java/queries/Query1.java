package queries;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import utils.HdfsUtility;
import utils.Query1Comparator;

public class Query1 {

		public static void run(SparkSession spark) {
			
			LocalDate last_dec = LocalDate.parse("2020-12-31", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			
			Dataset<Row> datasetSummary = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
	        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-summary-latest.parquet");
	        
	        Dataset<Row> datasetType = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
	        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/punti-somministrazione-tipologia.parquet");

	        datasetSummary.toJavaRDD().collect();

	        Instant start = Instant.now();
	        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();
	        JavaRDD<Row> rawType = datasetType.toJavaRDD();
	        
	        
	        JavaPairRDD<Tuple2<String, String>, Long> centriCount = rawType.mapToPair((row -> { 
	        	String area = row.getString(0);
	        	return new Tuple2<>(new Tuple2<>(area, row.getString(row.length()-1)), (long) 1);
	        })).reduceByKey((x, y) -> x+y).cache();
	        
	        JavaPairRDD<String, Tuple2<String, Long>> centriCountForJoin = centriCount.mapToPair((row -> { 
	        	return new Tuple2<>(row._1._1, new Tuple2<>(row._1._2, row._2));
	        })).cache();
	        
	        
	        //TODO filtering su giugno
	        
	        //Sort somministrazioni-vaccini-latest
	        JavaPairRDD<LocalDate, Tuple2<String, Long>> parsedSummary = rawSummary.mapToPair((row -> {
	            LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	            return new Tuple2<>(date, new Tuple2<>(row.getString(1), Long.valueOf(row.getInt(2))));
	        })).filter(row -> {
	        	return row._1.isAfter(last_dec);
	        }).sortByKey(true);
	        
	        
	        
	        // Month-Area-Totale
	        JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotal = parsedSummary.mapToPair((row -> {
	            Month month = row._1.getMonth();
	            return new Tuple2<>(new Tuple2<>(month, row._2._1), row._2._2);
	        })).reduceByKey((x, y) -> y+x);
	        
	        //MAp to pair preprocessing for join
	        JavaPairRDD<String, Tuple2<Month, Long>> monthAreaTotalForJoin = monthAreaTotal.mapToPair(row -> {
	        	return new Tuple2<> (row._1._2, new Tuple2<>(row._1._1, row._2));
	        }).cache();
	        
	        //Join
	        JavaPairRDD<String, Tuple2<Tuple2<Month, Long>, Tuple2<String, Long>>> monthAreaTotalJoin = monthAreaTotalForJoin.join(centriCountForJoin);

	       
	        //final result
			JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotalPerDay = monthAreaTotalJoin.mapToPair((row -> {
	            Month month = row._2._1._1;
	            LocalDate date = LocalDate.of(Year.now().getValue(), month, 1);
	            int lenghtOfMonth = date.lengthOfMonth();
	            
	            return new Tuple2<>(new Tuple2<>(month, row._2._2._1), row._2._1._2/(lenghtOfMonth*row._2._2._2));
	        })).sortByKey(new Query1Comparator<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder()));
	        
	        Instant end = Instant.now();
	        System.out.println(("Query completed in " + Duration.between(start, end).toMillis() + "ms"));
	        
	        JavaRDD<Row> resultJavaRDD = monthAreaTotalPerDay.map((Function<Tuple2<Tuple2<Month, String>, Long>, Row>) row -> {
				return RowFactory.create(row._1()._1().name(), row._1()._2(), row._2);
	        });
	        
	        List<StructField> resultFields = new ArrayList<>();
	        resultFields.add(DataTypes.createStructField("mese", DataTypes.StringType, false));
	        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
	        resultFields.add(DataTypes.createStructField("numero_medio_vaccini", DataTypes.LongType, false));
	        StructType resultStruct = DataTypes.createStructType(resultFields);
	        
	     // Saving performance results
	        Dataset<Row> dataset = spark.createDataFrame(resultJavaRDD, resultStruct);
	        HdfsUtility.write(dataset, HdfsUtility.QUERY1_DIR, SaveMode.Overwrite, false, "query1_results.parquet");
	        
	        if (ClassForTest.DEBUG) {
	        	HdfsUtility.writeForTest(dataset, HdfsUtility.QUERY1_DIR, SaveMode.Overwrite, false, "query1_results.csv");	 
	        }
	        /*List<Tuple2<Date, Tuple2<String, String>>> line =  parsedSummary.collect();
	        for (Tuple2<Date, Tuple2<String, String>> l:line) {
				System.out.println(l);
			}*/
	        
		}
		public static void main(String[] args) {
			SparkSession spark = SparkSession
	                .builder()
	                .appName("Test")
	                .config("spark.master", "local")
	                .config("spark.cores.max", 6)
	                .getOrCreate();
			Query1.run(spark);
		}

}

