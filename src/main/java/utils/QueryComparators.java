package utils;

import java.io.Serializable;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.text.SimpleDateFormat;

import scala.Tuple2;

public class QueryComparators<K, V> implements Comparator<Tuple2<K, V>>, Serializable{

	public static void main(String[] args) throws ParseException {
		SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                .config("spark.master", "local")
                .getOrCreate();
		
		Dataset<Row> dataset = spark.read().csv("/home/giuseppe/Scrivania/"
        		+ "prova.csv");
		JavaRDD<Row> dataJavaRDD = dataset.toJavaRDD();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		JavaRDD<String> strrJavaRDD = dataJavaRDD.map(row -> {
			return new String(row.getString(0) + "," + row.getString(1));
		});
		JavaRDD<LabeledPoint> JavaRDDLP = strrJavaRDD.map(line -> {
	        String[] parts = line.split(",");
	        LocalDate date = LocalDate.parse(parts[0], DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	        double[] feat = {(double) date.getDayOfYear()};
	        return new LabeledPoint(Double.valueOf(parts[parts.length - 1]), Vectors.dense(feat));
	    }).cache();
	
		List<LabeledPoint> line =  JavaRDDLP.take(100);
        for (LabeledPoint l:line) {
			System.out.println(l.toString());
		}
		// Building the model
	    int numIterations = 100;
	    final LinearRegressionModel model = 
	    		LinearRegressionWithSGD.train(JavaRDD.toRDD(JavaRDDLP), numIterations);
	    LocalDate date = LocalDate.parse("2020-03-14", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        double[] test = {(double) date.getDayOfYear()};
        System.out.println(test[0]);
	    double prediction = model.predict(Vectors.dense(test));
	    System.out.println(prediction);
	}
	
	private Comparator<K> comparator_K;
	private Comparator<V> comparator_V;

    public QueryComparators(Comparator<K> comparator_K, Comparator<V> comparator_V) {
        this.comparator_K = comparator_K;
        this.comparator_V = comparator_V;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
    	int c = comparator_K.compare(o1._1, o2._1);
    	if ( c == 0) {
        	return comparator_V.compare(o1._2, o2._2);
        }
    	return c;
    }

}
