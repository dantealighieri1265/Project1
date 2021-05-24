package query1;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import utils.QueryComparators;

public class Query1 {

		public static void main(String[] args) {
			SparkSession spark = SparkSession
	                .builder()
	                .appName("Query1")
	                .config("spark.master", "local")
	                .getOrCreate();

	        Dataset<Row> datasetSummary = spark.read().option("header","true").csv("/home/giuseppe/Scrivania/"
	        		+ "somministrazioni-vaccini-summary-latest.csv");
	        Dataset<Row> datasetType = spark.read().option("header","true").csv("/home/giuseppe/Scrivania/"
	        		+ "punti-somministrazione-tipologia.csv");

	        Instant start = Instant.now();
	        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();
	        JavaRDD<Row> rawType = datasetType.toJavaRDD();
	        
	        
	        JavaPairRDD<Tuple2<String, String>, Long> centriCount = rawType.mapToPair((row -> { 
	        	String area = row.getString(0);
	        	return new Tuple2<>(new Tuple2<>(area, row.getString(6)), (long) 1);
	        })).reduceByKey((x, y) -> x+y);
	        
	        JavaPairRDD<String, Tuple2<String, Long>> centriCountForJoin = centriCount.mapToPair((row -> { 
	        	return new Tuple2<>(row._1._1, new Tuple2<>(row._1._2, row._2));
	        })).cache();
	        
	        
	        
	        
	        //Sort somminastrazioni-vaccini-latest
	        JavaPairRDD<LocalDate, Tuple2<String, Long>> parsedSummary = rawSummary.mapToPair((row -> {
	            LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	            return new Tuple2<>(date, new Tuple2<>(row.getString(1), Long.valueOf(row.getString(2))));
	        })).sortByKey(true);
	        
	        
	        
	        // Month-Area-Totale
	        JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotal = parsedSummary.mapToPair((row -> {
	            Month month = row._1.getMonth();
	            return new Tuple2<>(new Tuple2<>(month, row._2._1), row._2._2);
	        })).reduceByKey((x, y) -> y+x);
	        
	        //MAp to pair preprocessing for join
	        JavaPairRDD<String, Tuple2<Month, Long>> monthAreaTotalForJoin = monthAreaTotal.mapToPair(row -> {
	        	return new Tuple2<> (row._1._2, new Tuple2<>(row._1._1, row._2));
	        });
	        
	        //Join
	        JavaPairRDD<String, Tuple2<Tuple2<Month, Long>, Tuple2<String, Long>>> monthAreaTotalJoin = monthAreaTotalForJoin.join(centriCountForJoin);

	       
	        //final result
	        JavaPairRDD<Tuple2<Month, String>, Long> monthAreaTotalPerDay = monthAreaTotalJoin.mapToPair((row -> {
	            Month month = row._2._1._1;
	            int[] i = {1, 3, 5, 7, 8, 10, 12};
	            Long nDay = (long) 0;
	            if (Arrays.asList(i).contains(month.getValue())) {
	            	nDay = (long) 31;
	            }else if (month.getValue() == 2){
	            	nDay = (long) 28;
				}else {
					nDay = (long) 30;
				}
	            return new Tuple2<>(new Tuple2<>(month, row._2._2._1), row._2._1._2/(nDay*row._2._2._2));
	        })).sortByKey(new QueryComparators<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder()));
	        
	        Instant end = Instant.now();
	        System.out.println(("Query completed in " + Duration.between(start, end).toMillis() + "ms"));
	        monthAreaTotalPerDay.saveAsTextFile("results");
	        /*List<Tuple2<Tuple2<String, String>, Long>> line =  monthAreaTotalPerDay.take(10);
	        
	        for (Tuple2<Tuple2<String, String>, Long> l:line) {
				System.out.println(l._1._1 +", "+l._1._2+", "+l._2);
			}*/
	        
	        
	        
	        /*List<Tuple2<Date, Tuple2<String, String>>> line =  parsedSummary.collect();
	        for (Tuple2<Date, Tuple2<String, String>> l:line) {
				System.out.println(l);
			}*/
	        
	        spark.close();
		}

}

