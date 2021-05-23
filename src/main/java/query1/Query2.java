package query1;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import utils.Query2Comparator;
import utils.QueryComparators;

public class Query2 {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> datasetVaccine = spark.read().option("header","true").csv("/home/marco/Scrivania/"
        		+ "somministrazioni-vaccini-latest.csv");
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD();
        
        //Eliminiamo i valori precedenti a Febbraio
        JavaRDD<Row> selectRow = rawVaccine.filter(row ->{
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return !date.isBefore(LocalDate.parse("2021-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")) );
        });
        
        /*PREPROCESSING:
         * - Fill dati mancanti
         * - Eliminare eventuali duplicati*/
        
        //Sommiamo i vaccini di marche diverse: [Data, Area, Age][vaccini]
        JavaPairRDD<Tuple3<LocalDate, String, String>, Long> sumOvervaccine = selectRow.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(new Tuple3<>(date, row.getString(row.length()-1), row.getString(3)), Long.valueOf(row.getString(5)));
        }).reduceByKey((x, y)-> x+y);
        
        //Sort by Date
        JavaPairRDD<LocalDate, Tuple3<String, String, Long>> sumOvervaccineSort = sumOvervaccine.mapToPair(row -> {
        	return new Tuple2<>(row._1._1(), new Tuple3<>(row._1._2(), row._1._3(), row._2));
        }).sortByKey();
        
        // (AREA, AGE)[(DATE, VACCINI)...]
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>> regionAge = sumOvervaccineSort.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._2._1(), row._2._2()), new Tuple2<>(row._1, row._2._3()));
        }).groupByKey();
        
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Integer, Long>>> regionAgeMonth = regionAge.flatMapToPair((PairFlatMapFunction<Tuple2<Tuple2<String, String>, 
        		Iterable<Tuple2<LocalDate, Long>>>, Tuple3<String, String, String>,  Tuple2<Integer, Long>>) row -> {
        			
        	Iterable<Tuple2<LocalDate, Long>> dayVaccines = row._2;
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> list = new ArrayList<>();
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> listSupport = new ArrayList<>();
        	int check = 0; 
        	int flag = 0;
        	int monthOld = 0;
        	for(Tuple2<LocalDate, Long> dayVaccine : dayVaccines) {
        		if (flag == 0) {
            		monthOld = dayVaccine._1.getMonthValue();
            		flag++;
        		}
        		
            	int month = dayVaccine._1.getMonthValue();
        		
        		
        		listSupport.add(new Tuple2<>(new Tuple3<>((String)row._1._1, (String)row._1._2, dayVaccine._1().getMonth().name()), 
    					new Tuple2<>(Integer.valueOf(dayVaccine._1().getDayOfYear()), dayVaccine._2)));
        		if (flag ==0) {
        			monthOld = month;
        			continue;
        		}
        		
        		if (month == monthOld) {
        			check++;
        		}else {
					if (check>=2) {
						list.addAll(listSupport);
					}else {
						System.out.println(row.toString());
					}
					listSupport.clear();
					check = 0;
				}
        		
        		monthOld = month;
        	}
        	return list.iterator();  	
        }).groupByKey();
        
        // (DATE, AGE, AREA)[(VACCINI)...]
        JavaPairRDD<Tuple3<Month, String, String>, Integer> regionAgeMonthRegression = regionAgeMonth.mapToPair(row -> {
        	SimpleRegression regression = new SimpleRegression();
        	int last = 0;
        	for (Tuple2<Integer, Long> point: row._2) {
				regression.addData(point._1, point._2);
				last = point._1;
			}
        	LocalDate lastDayFor =  Year.now().atDay(last);
        	LocalDate ld = lastDayFor.withDayOfMonth(lastDayFor.lengthOfMonth());
        	String day = String.valueOf(ld.plusDays(1).getDayOfMonth());
        	Month month = ld.plusDays(1).getMonth();
        	double prediction = regression.predict(ld.plusDays(1).getDayOfYear());
        	if (prediction<0.0) {
        		prediction = 0.0;
        	}
        	//System.out.println(ld.plusDays(1).getDayOfYear() +", "+ld.plusDays(1).toString()+" : "+ regression.getSlope()+", "+regression.getIntercept());
        	return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._1()), (int) prediction);
        	
        });
        
        JavaPairRDD<Tuple3<String, String, String>, Integer> result = regionAgeMonthRegression.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), new Tuple2<>(row._1._3(), row._2));
        }).groupByKey().mapToPair(row -> {
        
        	List<Tuple2<String, Integer>> list = new ArrayList<>();
        	row._2.forEach(list::add);
        	
        	list.sort(new Query2Comparator<String, Integer>(Comparator.reverseOrder()));
        	ArrayList<Tuple2<String, Integer>> listOrdered = new ArrayList<Tuple2<String, Integer>>(list.subList(0, 5));
        	Iterable<Tuple2<String, Integer>> i = listOrdered;
        	return new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), i);
        	
        }).sortByKey(new QueryComparators<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder())).flatMapToPair(row ->{
        	ArrayList<Tuple2<Tuple3<String, String, String>, Integer>> list = new ArrayList<>();
        	for (Tuple2<String, Integer> regVac : row._2) {
        		String month = "1 "+ row._1._1().name();
				list.add(new Tuple2<>(new Tuple3<>(month, row._1._2(), regVac._1), regVac._2));
			}
        	return list.iterator();
        });

        Instant end = Instant.now();
        System.out.println(("Query 2 completed in " + Duration.between(start, end).toMillis() + "ms"));
        
        List<Tuple2<Tuple3<String, String, String>, Integer>> line2 =  result.take(100);
        for (Tuple2<Tuple3<String, String, String>, Integer> l:line2) {
			System.out.println(l);
		}
        
        result.saveAsTextFile("Query2");
               
        
        
        
        spark.close();
	}

}
