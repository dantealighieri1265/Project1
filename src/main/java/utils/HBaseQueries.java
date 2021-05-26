package utils;

public class HBaseQueries {

	public static void main(String[] args) {
		HBaseClient hbc = new HBaseClient();
		hbc.createTable("query1", "mese", "area", "numero_medio_vaccini");
		System.out.println(hbc.describeTable("query1"));
		return;

	}

}
