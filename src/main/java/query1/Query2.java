package query1;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

public class Query2 {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> datasetVaccine = spark.read().option("header","true").csv("/home/marco/Scrivania/"
        		+ "somministrazioni-vaccini-latest.csv");
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD();
        
        JavaRDD<Row> selectRow = rawVaccine.filter(row ->{
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return !date.isBefore(LocalDate.parse("2021-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")) );
        });
        JavaPairRDD<Tuple3<LocalDate, String, String>, Long> sumOvervaccine = selectRow.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(new Tuple3<>(date, row.getString(row.length()-1), row.getString(3)), Long.valueOf(row.getString(5)));
        }).reduceByKey((x, y)-> x+y);
        
        JavaPairRDD<LocalDate, Tuple3<String, String, Long>> sumOvervaccineSort = sumOvervaccine.mapToPair(row -> {
        	return new Tuple2<>(row._1._1(), new Tuple3<>(row._1._2(), row._1._3(), row._2));
        }).sortByKey();
        
        List<Tuple2<LocalDate, Tuple3<String, String, Long>>> line =  sumOvervaccineSort.take(100);
        for (Tuple2<LocalDate, Tuple3<String, String, Long>> l:line) {
			System.out.println(l);
		}
        
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>> prova = sumOvervaccineSort.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._2._1(), row._2._2()), new Tuple2<>(row._1, row._2._3()));
        }).groupByKey();
        
        prova.saveAsTextFile("Query2");
        
        //Creare i Ttraining Set itera
        
        
        List<Tuple2<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>>> line2 =  prova.take(100);
        for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>> l:line2) {
			//System.out.println(l);
		}
        spark.close();
	}

}
