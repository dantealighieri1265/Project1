package query1;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

public class Query3 {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Query3")
                .config("spark.master", "local")
                .getOrCreate();
		LocalDate firstJune = LocalDate.parse("2021-06-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Dataset<Row> datasetVaccine = spark.read().option("header","true").csv("/home/giuseppe/Scrivania/"
        		+ "somministrazioni-vaccini-summary-latest.csv");
        Dataset<Row> datasetPopulation = spark.read().option("header","true").csv("/home/giuseppe/Scrivania/"
        		+ "totale-popolazione.csv");
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD().cache();
        JavaRDD<Row> rawPopulation = datasetPopulation.toJavaRDD().cache();
        
        JavaPairRDD<String, Iterable<Tuple2<LocalDate, Long>>> sumOvervaccine = rawVaccine.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(row.getString(row.length()-1), new Tuple2<>(date, Long.valueOf(row.getString(2))));
        }).groupByKey().sortByKey().cache();
        
        JavaPairRDD<Tuple2<String, String>, Integer> regionAgeMonthRegression = sumOvervaccine.mapToPair(row -> {
        	SimpleRegression regression = new SimpleRegression();
        	for (Tuple2<LocalDate, Long> point: row._2) {
        		int dateInt = point._1().getDayOfYear();
				regression.addData(dateInt, point._2());
			}
        	
        	double prediction = regression.predict(firstJune.getDayOfYear());
        	if (prediction<0.0) {
        		prediction = 0.0;
        	}
        	//System.out.println(ld.plusDays(1).getDayOfYear() +", "+ld.plusDays(1).toString()+" : "+ regression.getSlope()+", "+regression.getIntercept());
        	return new Tuple2<>(new Tuple2<>(row._1(), firstJune.getMonth().name()), (int) prediction);
        	
        });
        
        JavaPairRDD<Tuple2<String, String>, Long> population = rawPopulation.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row.getString(0), firstJune.getMonth().name()), Long.valueOf(row.getString(1)));
        });
        
        JavaPairRDD<Tuple2<String, String>, Double> sumOverArea = rawVaccine.mapToPair(row -> {
        	return new Tuple2<>(row.getString(row.length()-1), Long.valueOf(row.getString(2)));
        }).reduceByKey((x, y) -> (x+y)).mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1(), firstJune.getMonth().name()), row._2());
        }).join(regionAgeMonthRegression).mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1()._1(), firstJune.getMonth().name()), row._2()._1() + row._2()._2());
        }).join(population).mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1()._1(), firstJune.getMonth().name()), Double.valueOf(row._2()._1()) / Double.valueOf(row._2()._2()));
        }).cache();
        
        //clustering
        
        JavaRDD<Vector> training = sumOverArea.map(row -> {
        	return Vectors.dense(row._2());
        }).cache();
        
        ArrayList<JavaRDD<Tuple5>> listRDD = new ArrayList<>();
        ArrayList<Row> listPerformance = new ArrayList<Row>();
        
        for (int k = 2; k <= 5; k++) {
        	//K-Means
            Instant startKMeans = Instant.now();
        	KMeansModel cluster = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeans = Instant.now();
            Long KMeansTrainPerformance = Duration.between(startKMeans, endKMeans).toMillis();
            Double KMeansTrainCost = cluster.computeCost(training.rdd());
            
            
            listPerformance.add(RowFactory.create("K-MEANS", k, KMeansTrainPerformance, KMeansTrainCost));
            
            //K-Means
            
            Instant startKMeansBisectiong = Instant.now();
        	KMeansModel clusterBisectiong = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeansBisectiong = Instant.now();
            Long KMeansTrainPerformanceBisectiong = Duration.between(startKMeans, endKMeans).toMillis();
            Double KMeansTrainCostBisectiong = cluster.computeCost(training.rdd());
            
            listPerformance.add(RowFactory.create("K-MEANS-BISECTING", k, KMeansTrainPerformanceBisectiong, KMeansTrainCostBisectiong));
            
            JavaPairRDD<String, Integer> areaBelongTo = sumOverArea.mapToPair(row -> {
            	return new Tuple2<>(row._1()._1(), cluster.predict(Vectors.dense(row._2())));
            });	
		}
        
        
        
        KMeansModel cluster = KMeans.train(training.rdd(), 21, 100);
        JavaPairRDD<String, Integer> areaBelongTo = sumOverArea.mapToPair(row -> {
        	return new Tuple2<>(row._1()._1(), cluster.predict(Vectors.dense(row._2())));
        });
        
        System.out.println(cluster.computeCost(training.rdd())+", "+ cluster.trainingCost());

        Instant end = Instant.now();
        System.out.println(("Query 2 completed in " + Duration.between(start, end).toMillis() + "ms"));
        
        
        List<Tuple2<Tuple2<String, String>, Double>> line2 =  sumOverArea.take(100);
        for (Tuple2<Tuple2<String, String>, Double> l:line2) {
			System.out.println(l);
		}
        
        
        List<Tuple2<String, Integer>> line =  areaBelongTo.take(100);
        for (Tuple2<String, Integer> l:line) {
			System.out.println(l);
		}
        
        for (Vector center : cluster.clusterCenters()) {
			System.out.println(center);
		}
        
        //regionAgeMonthRegression.saveAsTextFile("Query3regression");
	}

}
