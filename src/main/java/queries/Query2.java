package queries;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.temporal.ChronoUnit;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.classification.InterfaceAudience.Public;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.Tuple3;
import utils.Query2Comparator;
import utils.HdfsUtility;
import utils.Query1Comparator;

public class Query2 {
	
	private static LocalDate FIRST_FEBRUARY = LocalDate.parse("2021-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	
	public static List<LocalDate> fillNa(LocalDate prevDay, LocalDate actualDay) {
		List<LocalDate> list = new ArrayList<LocalDate>();
		
		long daysBetween = ChronoUnit.DAYS.between(prevDay, actualDay);
		
		
		if(daysBetween > 1) {
			//ci sono righe mancanti
			for (int i = 0; i < daysBetween-1; i++) {
				list.add(prevDay.plusDays(i+1));
			}
			
		}
		
		return list;	
	}

	public static void run(SparkSession spark) {

        
        Dataset<Row> datasetVaccine = spark.read().option("header","true").parquet("hdfs:"+HdfsUtility.URL_HDFS+":" + 
        		HdfsUtility.PORT_HDFS+HdfsUtility.INPUT_HDFS+"/somministrazioni-vaccini-latest.parquet");
        
        Instant start = Instant.now();
        JavaRDD<Row> rawVaccine = datasetVaccine.toJavaRDD();
        List<Row> linePqrquet =  rawVaccine.take(100);
        for (Row l:linePqrquet) {
			System.out.println(l);
		}
        
        //Eliminiamo i valori precedenti a Febbraio
        JavaRDD<Row> selectRow = rawVaccine.filter(row ->{
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return !date.isBefore(FIRST_FEBRUARY);
        });
        
        /*PREPROCESSING:
         * - Fill dati mancanti
         * - Eliminare eventuali duplicati*/
        
        //Sommiamo i vaccini di marche diverse: [Data, Area, Age][vaccini]
        JavaPairRDD<Tuple3<LocalDate, String, String>, Long> sumOvervaccine = selectRow.mapToPair(row -> {
        	LocalDate date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        	return new Tuple2<>(new Tuple3<>(date, row.getString(row.length()-1), row.getString(3)), Long.valueOf(row.getInt(5)));
        }).reduceByKey((x, y)-> x+y);
        
        //sumOvervaccine.saveAsTextFile("prova");
        
        //Sort by Date
        JavaPairRDD<LocalDate, Tuple3<String, String, Long>> sumOvervaccineSort = sumOvervaccine.mapToPair(row -> {
        	return new Tuple2<>(row._1._1(), new Tuple3<>(row._1._2(), row._1._3(), row._2));
        }).sortByKey();
        
        // (AREA, AGE)[(DATE, VACCINI)...]
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<LocalDate, Long>>> regionAge = sumOvervaccineSort.mapToPair(row -> {
        	return new Tuple2<>(new Tuple2<>(row._2._1(), row._2._2()), new Tuple2<>(row._1, row._2._3()));
        }).groupByKey();
        
        
        
        //[mese, fascia, area, ][[data, vaccinati],...]
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Integer, Long>>> regionAgeMonth = regionAge.flatMapToPair((PairFlatMapFunction<Tuple2<Tuple2<String, String>, 
        		Iterable<Tuple2<LocalDate, Long>>>, Tuple3<String, String, String>,  Tuple2<Integer, Long>>) row -> {
        			
        	Iterable<Tuple2<LocalDate, Long>> dayVaccines = row._2;
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> list = new ArrayList<>();
        	ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<Integer, Long>>> listSupport = new ArrayList<>();
        	int check = 0; 
        	int flag = 0;
        	int monthOld = 0;
        	LocalDate prevDay = null;
        	
        	/*Controlla quanti giorni, considerato un mese specifico, una fascia specifica e 
        	 * una regione specifica, sono state effettuate almeno una vaccinazione ad una donna*/
        	int ckeckNoZeroWoman = 0; 
        	for(Tuple2<LocalDate, Long> dayVaccine : dayVaccines) {
        		if (flag == 0) {
            		monthOld = dayVaccine._1.getMonthValue();
            		//prevDay = LocalDate.of(2021, monthOld, 1);
            		
            		//System.out.println(dayVaccine._1.getMonth().name());
        		}
        		
            	int month = dayVaccine._1.getMonthValue();
        		
        		
        		listSupport.add(new Tuple2<>(new Tuple3<>((String)row._1._1, (String)row._1._2, dayVaccine._1().getMonth().name()), 
    					new Tuple2<>(Integer.valueOf(dayVaccine._1().getDayOfYear()), dayVaccine._2)));
        		       		
        		
        		if (flag ==0) {
        			//monthOld = month;
        			
        			/*LocalDate actualDay = dayVaccine._1;
        			List<LocalDate> dayNoVaccineWoman = fillNa(prevDay, actualDay);
        			for (LocalDate dayNo : dayNoVaccineWoman) {
        				listSupport.add(new Tuple2<>(new Tuple3<>((String)row._1._1, (String)row._1._2, dayVaccine._1().getMonth().name()), 
            					new Tuple2<>(Integer.valueOf(dayNo.getDayOfYear()), Long.valueOf(0))));
					}
        			prevDay = actualDay;*/
        			
        			flag++;
        			check++;
        			continue;
        		}
        		
        		if (month == monthOld) {
        			/*FillNa for regression*/
        			/*LocalDate actualDay = dayVaccine._1;
        			List<LocalDate> dayNoVaccineWoman = fillNa(prevDay, actualDay);
        			for (LocalDate dayNo : dayNoVaccineWoman) {
        				listSupport.add(new Tuple2<>(new Tuple3<>((String)row._1._1, (String)row._1._2, dayVaccine._1().getMonth().name()), 
            					new Tuple2<>(Integer.valueOf(dayNo.getDayOfYear()), Long.valueOf(0))));
					}
        			prevDay = actualDay;*/
        			
        			if (dayVaccine._2 != 0) {
        				ckeckNoZeroWoman++;
        			}
        			check++;
        		}else {
        			System.out.println(check+", "+dayVaccine._1.toString()+", "+row._1._1+", "+ dayVaccine._1.getMonth().name()+", "+row._1._2);
					if (check>=2 && ckeckNoZeroWoman>=2) {
						list.addAll(listSupport);
					}else {
						System.out.println("FASCIA DA NON CONSIDERARE: "+row.toString());
					}
					listSupport.clear();
					check = 0;
				}
        		monthOld = month;
        	}
        	
        	
        	/*Per l'ultimo mese considerato*/
        	//System.out.println(check+", "+dayVaccine._1.toString()+", "+row._1._1+", "+ dayVaccine._1.getMonth().name()+", "+row._1._2);
			if (check>=2 && ckeckNoZeroWoman>=2) {
				list.addAll(listSupport);
			}else {
				System.out.println("FASCIA DA NON CONSIDERARE: "+row.toString());
			}
			System.out.println(listSupport.get(1).toString());
			listSupport.clear();
			check = 0;
			
			
        	return list.iterator();  	
        }).groupByKey();
        
        //TODO
        /*Map in cui giriamo per un dato mese, tutti i giorni che sono presenti nel dataset, avendo cura di aggiungere i giorni mancanti e settare il numero di vaccini a 0*/
        
        
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
        	
        }).sortByKey(new Query1Comparator<Month, String>(Comparator.<Month>naturalOrder(), Comparator.<String>naturalOrder())).flatMapToPair(row ->{
        	ArrayList<Tuple2<Tuple3<String, String, String>, Integer>> list = new ArrayList<>();
        	for (Tuple2<String, Integer> regVac : row._2) {
        		String month = "1 "+ row._1._1().name();
				list.add(new Tuple2<>(new Tuple3<>(month, row._1._2(), regVac._1), regVac._2));
			}
        	return list.iterator();
        });

        Instant end = Instant.now();
        System.out.println(("Query 2 completed in " + Duration.between(start, end).toMillis() + "ms"));
        
        List<Tuple2<Tuple3<String, String, String>, Integer>> line3 =  result.take(100);
        for (Tuple2<Tuple3<String, String, String>, Integer> l:line3) {
			System.out.println(l);
		}
        
        //result.saveAsTextFile("Query2");
        JavaRDD<Row> resultJavaRDD = result.map(row -> {
			return RowFactory.create(row._1()._1(), row._1()._2(), row._1()._3(), row._2);
        });       
        List<StructField> resultFields = new ArrayList<>();
        resultFields.add(DataTypes.createStructField("giorno", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("fascia_eta", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        resultFields.add(DataTypes.createStructField("predizione_vaccini", DataTypes.IntegerType, false));
        StructType resultStruct = DataTypes.createStructType(resultFields);
        
     // Saving performance results
        Dataset<Row> dataset = spark.createDataFrame(resultJavaRDD, resultStruct);
        HdfsUtility.write(dataset, HdfsUtility.QUERY2_DIR, SaveMode.Overwrite, false, "query2_results.parquet");
        if (ClassForTest.DEBUG) {
            HdfsUtility.writeForTest(dataset, HdfsUtility.QUERY2_DIR, SaveMode.Overwrite, false, "query2_results.csv");
        }
       
	}
	public static void main(String[] args) {
		SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .config("spark.master", "local")
                .getOrCreate();
		LocalDate prevDay = LocalDate.parse("2021-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		LocalDate actualDay = LocalDate.parse("2021-02-03", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		long daysBetween = ChronoUnit.DAYS.between(prevDay, actualDay);
		System.out.println(daysBetween);
		System.out.println(prevDay = LocalDate.of(2021, 1, 1));
		//Query2.run(spark);
	}

}
