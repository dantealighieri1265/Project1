 package queries;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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

public class Query3 {

	public static void run(SparkSession spark) {
		
		LocalDate firstJune = LocalDate.parse("2021-06-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		Dataset<Row> datasetVaccine = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-summary-latest.parquet");
		
		Dataset<Row> datasetPopulation = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/totale-popolazione.parquet");
		datasetVaccine.toJavaRDD().collect();
		//TODO fill dei dati per regressione al 1 giugno
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD().cache();
        JavaRDD<Row> rawPopulation = datasetPopulation.toJavaRDD().cache();
        
        //Raggruppamento per area e ordinamento sull'area
        JavaPairRDD<String, Iterable<Tuple2<LocalDate, Long>>> groupByAreaSorted = rawVaccine.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(row.getString(row.length()-1), new Tuple2<>(date, Long.valueOf(row.getInt(2))));
        }).groupByKey().cache();
        
        //Regressione e predizione al 1 giugno per area
        JavaPairRDD<String, Integer> areaRegression = groupByAreaSorted.mapToPair(row -> {
        	SimpleRegression regression = new SimpleRegression();
        	for (Tuple2<LocalDate, Long> point: row._2) {
        		int dateInt = point._1().getDayOfYear();
				regression.addData(dateInt, point._2());
			}
        	
        	double prediction = regression.predict(firstJune.getDayOfYear());
        	if (prediction<0.0) {
        		prediction = 0.0;
        	}
        	return new Tuple2<>(row._1(), (int) prediction);
        	
        });
        
        //Preprocessing popolaziojne
        JavaPairRDD<String, Long> population = rawPopulation.mapToPair(row -> {
        	String area = row.getString(0);
        	if (area.contains("Valle")) {
				area = "Valle d'Aosta / Vallée d'Aoste";
			}
        	return new Tuple2<>(area, Long.valueOf(row.getInt(1)));
        });

        //Calcolo vaccini totali al 31 maggio, somma delle predizioni del 1 giugno e calcolo della percentuale
        //di popolazione vaccinata
        JavaPairRDD<Tuple2<String, String>, Double> predictionPercentage = rawVaccine.mapToPair(row -> {
        	return new Tuple2<>(row.getString(row.length()-1), Long.valueOf(row.getInt(2)));
        }).reduceByKey((x, y) -> (x+y)).mapToPair(row -> {
        	return new Tuple2<>(row._1(), row._2());
        }).join(areaRegression).mapToPair(row -> {
        	return new Tuple2<>(row._1(), row._2()._1() + row._2()._2());
        }).join(population).sortByKey().mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1(), "1 " + firstJune.getMonth().name()), Double.valueOf(row._2()._1()) / Double.valueOf(row._2()._2()));
        }).cache();
        
        JavaRDD<Row> resultJavaRDD = predictionPercentage.map(row -> {
			return RowFactory.create(row._1()._1(), row._1()._2(), row._2);
        });
        //clustering
        
        //TODO Per eliminare il costo di inizializzazione metterlo all'inzio
        
        JavaRDD<Vector> training = predictionPercentage.map(row -> {
        	return Vectors.dense(row._2());
        }).cache();
        
        KMeansModel cluster3 = KMeans.train(training.rdd(), 3, 100); 
        ArrayList<Row> listPerformance = new ArrayList<>();
        ArrayList<JavaRDD<Row>> listJavaRDD = new ArrayList<>();
        
        for (int k = 2; k <= 5; k++) {
        	//K-Means
            Instant startKMeans = Instant.now();
        	KMeansModel clusterKmeans = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeans = Instant.now();
            Long KMeansTrainPerformance = Duration.between(startKMeans, endKMeans).toMillis();
            Double KMeansTrainCost = clusterKmeans.computeCost(training.rdd());
            
            
            listPerformance.add(RowFactory.create("K-MEANS", k, KMeansTrainPerformance, KMeansTrainCost));
            //listRows.add(RowFactory.create("K-MEANS", k, row._1._1(), row._1._2(), clusterKmeans.predict(Vectors.dense(row._2()))));
            
            //K-Means Bisecting
            
            Instant startKMeansBisectiong = Instant.now();
        	KMeansModel clusterBisectiong = KMeans.train(training.rdd(), k, 100);
        	Instant endKMeansBisectiong = Instant.now();
            Long KMeansTrainPerformanceBisectiong = Duration.between(startKMeansBisectiong, endKMeansBisectiong).toMillis();
            Double KMeansTrainCostBisectiong = clusterBisectiong.computeCost(training.rdd());
            
            listPerformance.add(RowFactory.create("K-MEANS-BISECTING", k, KMeansTrainPerformanceBisectiong, KMeansTrainCostBisectiong));
            //listRows.add(RowFactory.create("K-MEANS-BISECTING", k, row._1._1(), row._1._2(), clusterBisectiong.predict(Vectors.dense(row._2()))));
            int k_support= k;
            JavaRDD<Row> areaBelongToKmeans = predictionPercentage.map(row -> {
            	return RowFactory.create("K-MEANS", k_support, row._1()._1(), row._2, clusterKmeans.predict(Vectors.dense(row._2())));
            });
            JavaRDD<Row> areaBelongToBisecting = predictionPercentage.map(row -> {
            	return RowFactory.create("K-MEANS-BISECTING", k_support, row._1()._1(), row._2, clusterBisectiong.predict(Vectors.dense(row._2())));
            });
            
            listJavaRDD.add(areaBelongToBisecting);
            listJavaRDD.add(areaBelongToKmeans);
            
		}
        
        Instant end = Instant.now();
        System.out.println(("Query 3 completed in " + Duration.between(start, end).toMillis() + "ms"));
       
        List<StructField> resultFields = new ArrayList<>();
        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("giorno", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("percentuale_vaccinati", DataTypes.DoubleType, false));
        StructType resultStruct = DataTypes.createStructType(resultFields);
        
     // Saving results
        Dataset<Row> dataset_results = spark.createDataFrame(resultJavaRDD, resultStruct);
        HdfsUtility.write(dataset_results, HdfsUtility.QUERY3_RESULTS_DIR, SaveMode.Overwrite, false, "query3_results.parquet");

        
        List<StructField> performanceFields = new ArrayList<>();
        performanceFields.add(DataTypes.createStructField("algorithm", DataTypes.StringType, false));
        performanceFields.add(DataTypes.createStructField("k", DataTypes.IntegerType, false));
        performanceFields.add(DataTypes.createStructField("performance_ms", DataTypes.LongType, false));
        performanceFields.add(DataTypes.createStructField("cost", DataTypes.DoubleType, false));
        StructType performanceStruct = DataTypes.createStructType(performanceFields);
        
     // Saving performance 
        Dataset<Row> dataset_performance = spark.createDataFrame(listPerformance, performanceStruct);
        HdfsUtility.write(dataset_performance, HdfsUtility.QUERY3_PERFORMANCE_DIR, SaveMode.Overwrite, false, "query3_performance.parquet");
        
        List<StructField> clusterResultFields = new ArrayList<>();
        clusterResultFields.add(DataTypes.createStructField("algorithm", DataTypes.StringType, false));
        clusterResultFields.add(DataTypes.createStructField("k", DataTypes.IntegerType, false));
        clusterResultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        clusterResultFields.add(DataTypes.createStructField("percentuale_vaccinati", DataTypes.DoubleType, false));
        clusterResultFields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, false));
        StructType clusterResultStruct = DataTypes.createStructType(clusterResultFields);
        
     // Saving cluster
        Dataset<Row> dataset_cluster = null;
        for (JavaRDD<Row> rdd : listJavaRDD) {
        	 dataset_cluster = spark.createDataFrame(rdd, clusterResultStruct);
        	HdfsUtility.write(dataset_cluster, HdfsUtility.QUERY3_CLUSTER_DIR+"_Support", SaveMode.Append, false, "_Support");
        	if (ClassForTest.DEBUG) {
            	HdfsUtility.writeForTest(dataset_cluster, HdfsUtility.QUERY3_CLUSTER_DIR, SaveMode.Append, false, "query3_cluster.csv");
        	}
		}
        
        /*Dataset<Row> df= spark.read().parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.OUTPUT_HDFS+HdfsUtility.QUERY3_CLUSTER_DIR+"_Support");*/
        Dataset<Row>df = HdfsUtility.read(spark, HdfsUtility.QUERY3_CLUSTER_DIR+"_Support", HdfsUtility.OUTPUT_HDFS);
        Dataset<Row> df_output=df.coalesce(1);
        HdfsUtility.write(df_output, HdfsUtility.QUERY3_CLUSTER_DIR, SaveMode.Overwrite, true, "query3_cluster.parquet");
        
        
        
        /*List<Tuple2<String, Iterable<Tuple2<LocalDate, Long>>>> line2 =  groupByAreaSorted.collect();
        for (Tuple2<String, Iterable<Tuple2<LocalDate, Long>>> l:line2) {
			System.out.println(l +", "+ Iterables.size(l._2()));
		}*/
        
        if (ClassForTest.DEBUG) {
            HdfsUtility.writeForTest(dataset_performance, HdfsUtility.QUERY3_PERFORMANCE_DIR, SaveMode.Overwrite, false, "query3_performance.csv");
            HdfsUtility.writeForTest(dataset_results, HdfsUtility.QUERY3_RESULTS_DIR, SaveMode.Overwrite, false, "query3_results.csv");
           
        }
        


        
                /*List<Tuple2<String, Integer>> line =  areaBelongTo.take(100);
        for (Tuple2<String, Integer> l:line) {
			System.out.println(l);
		}
        
        for (Vector center : cluster.clusterCenters()) {
			System.out.println(center);
		}*/
        
        //regionAgeMonthRegression.saveAsTextFile("Query3regression");
	}
	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		Query3.run(spark);
	}

}
