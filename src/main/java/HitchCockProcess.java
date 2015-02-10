import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.Lists;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class HitchCockProcess implements Serializable {
	private static final JavaDoubleRDD cassndraRowsRDD = null;
	private transient SparkConf conf;

	// private int xSize, ySize, zSize, subjectSize;

	private HitchCockProcess(SparkConf conf) {
		this.conf = conf;

	}

	private void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		// this.xSize = getXSize(sc);
		// this.ySize = getYSize(sc);
		// this.zSize = getZSize(sc);
		// this.subjectSize = getSubjectSize(sc);
		testGetData(sc);

		/*
		 * SparkContextJavaFunctions functions = CassandraJavaUtil
		 * .javaFunctions(sc); JavaRDD<CassandraRow> rdd =
		 * functions.cassandraTable("engagement", "hitchcockdatatotal"); //
		 * 1/23/2015 JavaPairRDD<Integer, Integer> sizes = rdd .groupBy(new
		 * Function<CassandraRow, Integer>() {
		 * 
		 * @Override public Integer call(CassandraRow row) throws Exception {
		 * return row.getInt("subject"); } }) .mapToPair( new
		 * PairFunction<Tuple2<Integer, Iterable<CassandraRow>>, Integer,
		 * Integer>() { public Tuple2<Integer, Integer> call( Tuple2<Integer,
		 * Iterable<CassandraRow>> t) throws Exception { return new
		 * Tuple2<Integer, Integer>(t._1(), Lists.newArrayList(t._2()).size());
		 * } }); sizes.cache();
		 * 
		 * /* JavaRDD<String> rdd1; JavaRDD<String> rdd2; rdd1 =
		 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
		 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new
		 * Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } }); rdd2 =
		 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
		 * .where("x=? AND y=? AND z=? ", 2, 3, 4) .map(new
		 * Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } });
		 */

		/*
		 * JavaRDD<Hitchcockdatatotal> rdd1 = javaFunctions(sc).cassandraTable(
		 * "engagement", "hitchcockdatatotal",
		 * mapRowTo(Hitchcockdatatotal.class)).where( "x=? AND y=? AND z=? ", 1,
		 * 2, 3); JavaRDD<Hitchcockdatatotal> rdd2 =
		 * javaFunctions(sc).cassandraTable( "engagement", "hitchcockdatatotal",
		 * mapRowTo(Hitchcockdatatotal.class)).where( "x=? AND y=? AND z=? ", 4,
		 * 5, 6); gatherData(sc, rdd1, rdd2); compute(sc, rdd1, rdd2);
		 * showResults(sc); sc.stop();
		 */
	}

	public int getXSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> xS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.select("x").distinct();
		System.out.println("X size: " + xS.count());
		return (int) xS.count();
	}

	public int getYSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> yS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.where("x=?", "0").select("y").distinct();
		System.out.println("Y size: " + yS.count());
		return (int) yS.count();
	}

	public int getZSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> zS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.where("x=? and y=?", "0", "0").select("z").distinct();
		System.out.println("Z size: " + zS.count());
		return (int) zS.count();
	}

	public int getSubjectSize(JavaSparkContext sc) {
		JavaRDD<CassandraRow> subjectS = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal2")
				.select("subject").distinct();
		System.out.println("subject size: " + subjectS.count());
		return (int) subjectS.count();
	}

	public void testGetData(JavaSparkContext sc) {
		// JavaPairRDD<Hitchcockdatatotal,Integer> newRDD =
		// javaFunctions(sc).cassandraTable("engagement","hitchcockdatatotal");

		JavaPairRDD<String, List<Double>> s = javaFunctions(sc)
				.cassandraTable("engagement", "hitchcockdatatotal",
						mapRowTo(Hitchcockdatatotal.class))
				.where("subject =?", "0")
				.sortBy(new Function<Hitchcockdatatotal, Double>() {
					@Override
					public Double call(Hitchcockdatatotal v1) throws Exception {

						return v1.getTime() + 0.0;
					}
				}, true, 1)
				.mapToPair(
						new PairFunction<Hitchcockdatatotal, String, List<Double>>() {
							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, List<Double>> call(
									Hitchcockdatatotal t) throws Exception {
								// TODO Auto-generated method stub
								ArrayList<Double> temp = new ArrayList<Double>();
								temp.add(t.getData() + 0.0);
								return new Tuple2<String, List<Double>>(t
										.getID(), temp);
							}
						})
				.reduceByKey(
						new Function2<List<Double>, List<Double>, List<Double>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public List<Double> call(List<Double> v1,
									List<Double> v2) throws Exception {
								ArrayList<Double> temp = new ArrayList<Double>();
								temp.addAll(v1);
								temp.addAll(v2);
								return temp;
							}
						}).sortByKey();
						//.coalesce(2000).cache();
		JavaPairRDD<Tuple2<String, List<Double>>, Tuple2<String, List<Double>>> temp = s
				.cartesian(s);
		JavaRDD<Tuple2<String, Double>> corrData = temp
				.map(new Function<Tuple2<Tuple2<String, List<Double>>, Tuple2<String, List<Double>>>, Tuple2<String, Double>>() {

					@Override
					public Tuple2<String, Double> call(
							Tuple2<Tuple2<String, List<Double>>, Tuple2<String, List<Double>>> t)
							throws Exception {

						Double c = getPearsonCorrelation(t._1._2, t._2._2);
						// TODO Auto-generated method stub
						return new Tuple2<String, Double>(t._1._1 + ":"
								+ t._2._1, c);
					}
				});
		try {
			FileWriter fw = new FileWriter("preCorr.txt", false);
			BufferedWriter bf = new BufferedWriter(fw);
			bf.write(corrData.toArray().toString());
			bf.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("hitchcock process finished");
			//s.unpersist();
		}

		/*
		 * JavaPairRDD<String, ArrayList<Integer>> arrayS0 = s0 .reduceByKey(new
		 * Function2<ArrayList<Integer>, Integer, Integer>() {
		 * 
		 * @Override public Integer call(ArrayList<Integer> v1, Integer v2)
		 * throws Exception { // TODO Auto-generated method stub return null; }
		 * 
		 * }); /* JavaRDD<String> cassandraRowsRDD = javaFunctions(sc)
		 * .cassandraTable("engagement", "hitchcockdatatotal")
		 * .where("x=? and y=? and z=? and subject=? ", "0", "0", "0",
		 * "17").map(new Function<CassandraRow, String>() {
		 * 
		 * @Override public String call(CassandraRow cassandraRow) throws
		 * Exception { return cassandraRow.toString(); } });
		 * 
		 * /* System.out.println("Data as CassandraRows at 0,0,0,17: \n" +
		 * StringUtils.join(cassndraRowsRDD.toArray(), "\n"));
		 */
		/*
		 * System.out
		 * .println("Data as CassandraRows at subject=0 and time=0: \n" +
		 * StringUtils.join(s.toArray(), "\n")); System.out
		 * .println("Data as CassandraRows after converting to vectors : \n" +
		 * StringUtils.join(corrData.toArray(), "\n"));
		 */
	}

	public double getPearsonCorrelation(List<Double> scores1,
			List<Double> scores2) {

		double result = 0;

		double sum_sq_x = 0;

		double sum_sq_y = 0;

		double sum_coproduct = 0;

		double mean_x = scores1.get(0);

		double mean_y = scores2.get(0);

		for (int i = 2; i < scores1.size() + 1; i += 1) {

			double sweep = Double.valueOf(i - 1) / i;

			double delta_x = scores1.get(i - 1) - mean_x;

			double delta_y = scores2.get(i - 1) - mean_y;

			sum_sq_x += delta_x * delta_x * sweep;

			sum_sq_y += delta_y * delta_y * sweep;

			sum_coproduct += delta_x * delta_y * sweep;

			mean_x += delta_x / i;

			mean_y += delta_y / i;

		}

		double pop_sd_x = (double) Math.sqrt(sum_sq_x / scores1.size());

		double pop_sd_y = (double) Math.sqrt(sum_sq_y / scores1.size());

		double cov_x_y = sum_coproduct / scores1.size();

		result = cov_x_y / (pop_sd_x * pop_sd_y);

		return result;
	}

	/*
	 * public void gatherData(JavaSparkContext sc, JavaRDD<Hitchcockdatatotal>
	 * rdd1, JavaRDD<Hitchcockdatatotal> rdd2) { JavaRDD<String>
	 * cassandraRowsRDD1 = javaFunctions(sc) .cassandraTable("engagement",
	 * "hitchcocktotal") .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new
	 * Function<CassandraRow, String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } }); JavaRDD<String> cassandraRowsRDD2
	 * = javaFunctions(sc) .cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } });
	 * System.out.println("Data as CassandraRows: \n" +
	 * StringUtils.join(cassandraRowsRDD1.toArray(), "\n"));
	 */
	// another way to read in data:

	/*
	 * rdd1 = javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 1, 2, 3) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } }); rdd2 =
	 * javaFunctions(sc).cassandraTable("engagement", "hitchcocktotal")
	 * .where("x=? AND y=? AND z=? ", 2, 3, 4) .map(new Function<CassandraRow,
	 * String>() {
	 * 
	 * @Override public String call(CassandraRow cassandraRow) throws Exception
	 * { return cassandraRow.toString(); } });
	 * 
	 * 
	 * } /* private void compute(JavaSparkContext sc,
	 * JavaRDD<Hitchcockdatatotal> rdd1, JavaRDD<Hitchcockdatatotal> rdd2) {
	 * 
	 * // compute the correlation using Pearson's method. Enter "spearman" for
	 * // Spearman's method. If a // method is not specified, Pearson's method
	 * will be used by default. // Double correlation =
	 * Statistics.corr(rdd1.rdd(), rdd2.rdd(), // "pearson");
	 * 
	 * }
	 */
	private void showResults(JavaSparkContext sc) {
	}

	public static void main(String args[]) {
		
		
		
		
		/*
		 * to set the username: .set("spark.cassandra.username", "cassandra")
		 * //Optional to set the password: .set("spark.cassandra.password",
		 * "cassandra") //Optional
		 */
		SparkConf conf = new SparkConf();
		conf.setAppName("hitchcockprocess");
		// local[4] is not a spark cluster.
		conf.setMaster("spark://wolf.iems.northwestern.edu:7077");
		// cub0 is the cassandra cluster
		conf.set("spark.cassandra.connection.host", "cub0");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.executor.memory", "20g");
		HitchCockProcess app = new HitchCockProcess(conf);
		app.run();
	}
}