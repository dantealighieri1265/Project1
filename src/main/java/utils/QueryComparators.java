package utils;

import java.io.Serializable;
import java.text.DateFormatSymbols;
import java.time.Month;
import java.util.Comparator;
import java.util.Date;

import scala.Tuple2;

public class QueryComparators<K, V> implements Comparator<Tuple2<K, V>>, Serializable{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String month_s = new DateFormatSymbols().getMonths()[11];
		System.out.println(month_s);
		Date date = new Date();
		Date date1 = new Date();
        date.setMonth(3);
        date1.setMonth(2);
        System.out.println(date.after(date1));
        Month m = Month.of(3);
        Month m1 = Month.of(3);
        System.out.println(m.compareTo(m1));
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
